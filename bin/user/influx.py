# Copyright 2016-2021 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

"""
Influx is a platform for collecting, storing, and managing time-series data.

http://influxdata.com

This is a weewx extension that uploads data to an Influx server.

Database Configuration and Access

When it starts up, this extension will attempt to create the influx database.

To disable database creation, set create_database=False.

If credentials for a database administrator were provided, it will use those
credentials.  Otherwise, it will use the username/password credentials.

All other access to the influx database uses the username/password credentials
(non database administrator), if provided.

Minimal Configuration

A database name is required.  All weewx variables will be uploaded using weewx
names and default units and formatting.

[StdRESTful]
    [[Influx]]
        host = influxservername.example.com
        database = DATABASE

Customization: line format and database structure

This uploader supports a few formats, using either the single- or multi-line
format in influx.  Options for this parameter include:  multi-line,
multi-line-dotted, and single-line.  The default is single-line.

[StdRESTful]
    [[Influx]]
        measurement = weewx
        line_format = multi-line-dotted

Here are examples of the data sent to influx when the 'measurement' parameter
is set to 'weewx'.

The single-line format results in the following:

  weewx[tags] name0=x,name1=y,name2=z ts

The multi-line format results in the following:

  name0[tags] value=x ts
  name1[tags] value=y ts
  name2[tags] value=z ts

The multi-line-dotted format results in the following:

  weewx.name0[tags] value=x ts
  weewx.name1[tags] value=x ts
  weewx.name2[tags] value=x ts

Which format should you use?  It depends on how you want the data to end up in
influx.  For influx, think of measurement name as table, tags as column names,
and fields as unindexed columns.

For example, consider these data points:

{'H19': 528, 'VPV': 63.68, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.41, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.68, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 14.43, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.71, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.43, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.74, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.43, 'CS': 5, 'PPV': 9}

A single-line configuration results in this:

> select * from value
name: value
time                CS H19 H20 H21 H22 H23 I   PPV V     VPV   binding
----                -- --- --- --- --- --- -   --- -     ---   -------
1536086335000000000 5  528 19  115 23  93  0.6 9   13.41 63.68 loop   
1536086337000000000 5  528 19  115 23  93  0.6 9   13.43 63.68 loop   
1536086339000000000 5  528 19  115 23  93  0.6 9   13.43 63.71 loop   
1536086341000000000 5  528 19  115 23  93  0.6 9   13.43 63.74 loop   

A multi-line configuration results in this:

> select * from VPV
name: value
time                VPV
----                ---
1536086335000000000 63.68
1536086337000000000 63.68
1536086339000000000 63.71
1536086341000000000 63.74

Customization: controlling which variables are uploaded

When an input map is specified, only variables in that map will be uploaded.
The 'units' parameter can be used to specify which units should be used for
the input, independent of the local weewx units.

[StdRESTful]
    [[Influx]]
        database = DATABASE
        host = localhost
        port = 8086
        tags = station=A
        [[[inputs]]]
            [[[[barometer]]]]
                units = inHg
                name = barometer_inHg
                format = %.3f
            [[[[outTemp]]]]
                units = degree_F
                name = outTemp_F
                format = %.1f
            [[[[outHumidity]]]]
                name = outHumidity
                format = %03.0f
            [[[[windSpeed]]]]
                units = mph
                name = windSpeed_mph
                format = %.2f
            [[[[windDir]]]]
                format = %03.0f
"""

try:
    # Python 3
    import queue
except ImportError:
    # Python 2
    import Queue as queue
import base64
from distutils.version import StrictVersion
try:
    # Python 3
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
import socket
try:
    # Python 3
    from urllib.parse import urlparse, urlencode
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
except ImportError:
    # Python 2
    from urlparse import urlparse
    from urllib import urlencode
    from urllib2 import urlopen, Request, HTTPError, URLError
    from httplib import BadStatusLine
import sys

import weewx
import weewx.restx
import weewx.units
from weeutil.weeutil import to_bool, accumulateLeaves

VERSION = "0.17"

REQUIRED_WEEWX = "3.5.0"
if StrictVersion(weewx.__version__) < StrictVersion(REQUIRED_WEEWX):
    raise weewx.UnsupportedFeature("weewx %s or greater is required, found %s"
                                   % (REQUIRED_WEEWX, weewx.__version__))

try:
    # Test for new-style weewx logging by trying to import weeutil.logger
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__)

    def logdbg(msg):
        log.debug(msg)

    def loginf(msg):
        log.info(msg)

    def logerr(msg):
        log.error(msg)

except ImportError:
    # Old-style weewx logging
    import syslog

    def logmsg(level, msg):
        syslog.syslog(level, 'restx: Influx: %s' % msg)

    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)

    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)

    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

# some unit labels are rather lengthy.  this reduces them to something shorter.
UNIT_REDUCTIONS = {
    'degree_F': 'F',
    'degree_C': 'C',
    'inch': 'in',
    'mile_per_hour': 'mph',
    'mile_per_hour2': 'mph',
    'km_per_hour': 'kph',
    'km_per_hour2': 'kph',
    'knot': 'knot',
    'knot2': 'knot',
    'meter_per_second': 'mps',
    'meter_per_second2': 'mps',
    'degree_compass': None,
    'watt_per_meter_squared': 'Wpm2',
    'uv_index': None,
    'percent': None,
    'unix_epoch': None,
}

# observations that should be skipped when obs_to_upload is 'most'
OBS_TO_SKIP = ['dateTime', 'interval', 'usUnits']

MAX_SIZE = 1000000

# return the units label for an observation
def _get_units_label(obs, unit_system, unit_type=None):
    if unit_type is None:
        (unit_type, _) = weewx.units.getStandardUnitType(unit_system, obs)
    return UNIT_REDUCTIONS.get(unit_type, unit_type)

# get the template for an observation based on the observation key
def _get_template(obs_key, overrides, append_units_label, unit_system):
    tmpl_dict = dict()
    if append_units_label:
        unit_type = overrides.get('units')
        label = _get_units_label(obs_key, unit_system, unit_type)
        if label is not None:
            tmpl_dict['name'] = "%s_%s" % (obs_key, label)
    for x in ['name', 'format', 'units']:
        if x in overrides:
            tmpl_dict[x] = overrides[x]
    return tmpl_dict


class Influx(weewx.restx.StdRESTbase):
    def __init__(self, engine, cfg_dict):
        """This service recognizes standard restful options plus the following:

        Required parameters:

        database: name of the database at the server

        Optional parameters:

        host: server hostname
        Default is localhost

        port: server port
        Default is 8086

        server_url: full restful endpoint of the server
        Default is None

        measurement: name of the measurement
        Default is 'record'

        tags: comma-delimited list of name=value pairs to identify the
        measurement.  tags cannot contain spaces.
        Default is None

        create_database: should the upload attempt to create database first
        Default is True

        line_format: which line protocol format to use.  Possible values are
        single-line, multi-line, or multi-line-dotted.
        Default is single-line

        append_units_label: should units label be appended to name
        Default is True

        obs_to_upload: Which observations to upload.  Possible values are
        most, none, or all.  When none is specified, only items in the inputs
        list will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is most

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None

        binding: options include "loop", "archive", or "loop,archive"
        Default is archive
        """
        super(Influx, self).__init__(engine, cfg_dict)
        loginf("service version is %s" % VERSION)
        site_dict = weewx.restx.get_site_dict(cfg_dict, 'Influx', 'database')
        if site_dict is None:
            return


        port = int(site_dict.get('port', 8086))
        host = site_dict.get('host', 'localhost')
        if site_dict.get('server_url', None) is None:
            site_dict['server_url'] = 'http://%s:%s' % (host, port)
        site_dict.pop('host', None)
        site_dict.pop('port', None)
        site_dict.setdefault('username', None)
        site_dict.setdefault('password', '')
        site_dict.setdefault('dbadmin_username', None)
        site_dict.setdefault('dbadmin_password', '')
        site_dict.setdefault('create_database', True)
        site_dict.setdefault('tags', None)
        site_dict.setdefault('line_format', 'single-line')
        site_dict.setdefault('obs_to_upload', 'most')
        site_dict.setdefault('append_units_label', True)
        site_dict.setdefault('augment_record', True)
        site_dict.setdefault('measurement', 'record')

        loginf("database: %s" % site_dict['database'])
        loginf("destination: %s" % site_dict['server_url'])
        loginf("line_format: %s" % site_dict['line_format'])
        loginf("measurement: %s" % site_dict['measurement'])

        site_dict['append_units_label'] = to_bool(
            site_dict.get('append_units_label'))
        site_dict['augment_record'] = to_bool(site_dict.get('augment_record'))

        usn = site_dict.get('unit_system', None)
        if usn in weewx.units.unit_constants:
            site_dict['unit_system'] = weewx.units.unit_constants[usn]
            loginf("desired unit system: %s" % usn)

        if 'inputs' in cfg_dict['StdRESTful']['Influx']:
            site_dict['inputs'] = dict(
                cfg_dict['StdRESTful']['Influx']['inputs'])

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        try:
            if site_dict.get('augment_record'):
                _manager_dict = weewx.manager.get_manager_dict_from_config(
                    cfg_dict, 'wx_binding')
                site_dict['manager_dict'] = _manager_dict
        except weewx.UnknownBinding:
            pass

        if 'tags' in site_dict:
            if isinstance(site_dict['tags'], list):
                site_dict['tags'] = ','.join(site_dict['tags'])
            loginf("tags: %s" % site_dict['tags'])

        # we can bind to loop packets and/or archive records
        binding = site_dict.pop('binding', 'archive')
        if isinstance(binding, list):
            binding = ','.join(binding)
        loginf('binding: %s' % binding)

        data_queue = queue.Queue()
        try:
            data_thread = InfluxThread(data_queue, **site_dict)
        except weewx.ViolatedPrecondition as e:
            loginf("Data will not be posted: %s" % e)
            return
        data_thread.start()

        if 'loop' in binding.lower():
            self.loop_queue = data_queue
            self.loop_thread = data_thread
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        if 'archive' in binding.lower():
            self.archive_queue = data_queue
            self.archive_thread = data_thread
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        loginf("Data will be uploaded to %s" % site_dict['server_url'])

    def new_loop_packet(self, event):
        data = {'binding': 'loop'}
        data.update(event.packet)
        self.loop_queue.put(data)

    def new_archive_record(self, event):
        data = {'binding': 'archive'}
        data.update(event.record)
        self.archive_queue.put(data)


class InfluxThread(weewx.restx.RESTThread):

    _DEFAULT_SERVER_URL = 'http://localhost:8086'

    def __init__(self, queue, database,
                 username=None, password=None,
                 dbadmin_username=None, dbadmin_password=None,
                 line_format='single-line', create_database=True,
                 measurement='record', tags=None,
                 unit_system=None, augment_record=True,
                 inputs=dict(), obs_to_upload='most', append_units_label=True,
                 server_url=_DEFAULT_SERVER_URL, skip_upload=False,
                 manager_dict=None,
                 post_interval=None, max_backlog=MAX_SIZE, stale=None,
                 log_success=True, log_failure=True,
                 timeout=60, max_tries=3, retry_wait=5):
        super(InfluxThread, self).__init__(queue,
                                           protocol_name='Influx',
                                           manager_dict=manager_dict,
                                           post_interval=post_interval,
                                           max_backlog=max_backlog,
                                           stale=stale,
                                           log_success=log_success,
                                           log_failure=log_failure,
                                           max_tries=max_tries,
                                           timeout=timeout,
                                           retry_wait=retry_wait)
        self.database = database
        self.username = username
        self.password = password
        self.measurement = measurement
        self.tags = tags
        self.obs_to_upload = obs_to_upload
        self.append_units_label = append_units_label
        self.inputs = inputs
        self.server_url = server_url
        self.skip_upload = to_bool(skip_upload)
        self.unit_system = unit_system
        self.augment_record = augment_record
        self.templates = dict()
        self.line_format = line_format

        if create_database:
            uname = None
            pword = None
            if dbadmin_username:
                uname = dbadmin_username
                pword = dbadmin_password
            elif username:
                uname = username
                pword = password
            self.create_database(uname, pword)

    def create_database(self, username, password):
        # ensure that the database exists
        qstr = urlencode({'q': 'CREATE DATABASE %s' % self.database})
        url = '%s/query?%s' % (self.server_url, qstr)
        req = Request(url)
        req.add_header("User-Agent", "weewx/%s" % weewx.__version__)
        if username and password:
            # Create a base64 byte string with the authorization info
            base64bytes = base64.b64encode(('%s:%s' % (self.username, self.password)).encode())
            # Add the authentication header to the request:
            req.add_header("Authorization", b"Basic %s" % base64bytes)
        try:
            # The use of a GET to create a database has been deprecated.
            # Include a dummy payload to force a POST.
            self.post_request(req, 'None')
        except (socket.error, socket.timeout, URLError, http_client.BadStatusLine, http_client.IncompleteRead) as e:
            logerr("create database failed: %s" % e)

    def get_record(self, record, dbm):
        # We allow the superclass to add stuff to the record only if the user
        # requests it
        if self.augment_record and dbm:
            record = super(InfluxThread, self).get_record(record, dbm)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        return record

    def format_url(self, _):
        return '%s/write?db=%s' % (self.server_url, self.database)

    def get_request(self, url):
        """Override and add username and password"""

        # Get the basic Request from my superclass
        request = super(InfluxThread, self).get_request(url)

        if self.username and self.password:
            # Create a base64 byte string with the authorization info
            base64string = base64.b64encode(('%s:%s' % (self.username, self.password)).encode())
            # Add the authentication header to the request:
            request.add_header("Authorization", b"Basic %s" % base64string)
        return request

    def check_response(self, response):
        if response.code == 204:
            return
        payload = response.read().decode()
        if payload and payload.find('results') >= 0:
            logdbg("code: %s payload: %s" % (response.code, payload))
            return
        raise weewx.restx.FailedPost("Server returned '%s' (%s)" %
                                     (payload, response.code))

    def handle_exception(self, e, count):
        if isinstance(e, HTTPError):
            payload = e.read().decode()
            logdbg("exception: %s payload: %s" % (e, payload))
            if payload and payload.find("error") >= 0:
                if payload.find("database not found") >= 0:
                    raise weewx.restx.AbortedPost(payload)
        super(InfluxThread, self).handle_exception(e, count)

    def post_request(self, request, payload=None):
        # FIXME: provide full set of ssl options instead of this hack
        if self.server_url.startswith('https'):
            import ssl
            encoded = None
            if payload:
                encoded = payload.encode('utf-8')
            return urlopen(request, data=encoded, timeout=self.timeout,
                           context=ssl._create_unverified_context())
        return super(InfluxThread, self).post_request(request, payload)

    def get_post_body(self, record):
        """Override my superclass and get the body of the POST"""

        # create the list of tags
        tags = ''
        binding = record.pop('binding', None)
        if binding is not None:
            tags = ',binding=%s' % binding
        if self.tags:
            tags = '%s,%s' % (tags, self.tags)

        # if uploading everything, we must check every time the list of
        # variables that should be uploaded since variables may come and
        # go in a record.  use the inputs to override any generic template
        # generation.
        if self.obs_to_upload == 'all' or self.obs_to_upload == 'most':
            for f in record:
                if self.obs_to_upload == 'most' and f in OBS_TO_SKIP:
                    continue
                if f not in self.templates:
                    self.templates[f] = _get_template(f,
                                                      self.inputs.get(f, {}),
                                                      self.append_units_label,
                                                      record['usUnits'])

        # otherwise, create the list of upload variables once, based on the
        # user-specified list of inputs.
        elif not self.templates:
            for f in self.inputs:
                self.templates[f] = _get_template(f, self.inputs[f],
                                                  self.append_units_label,
                                                  record['usUnits'])

        # loop through the templates, populating them with data from the
        # record.
        data = []
        for k in self.templates:
            try:
                v = float(record.get(k))
                name = self.templates[k].get('name', k)
                fmt = self.templates[k].get('format', '%s')
                to_units = self.templates[k].get('units')
                if to_units is not None:
                    (from_unit, from_group) = weewx.units.getStandardUnitType(
                        record['usUnits'], k)
                    from_t = (v, from_unit, from_group)
                    v = weewx.units.convert(from_t, to_units)[0]
                s = fmt % v
                if self.line_format == 'multi-line-dotted':
                    # use multiple lines with a dotted-name identifier
                    n = "%s.%s" % (self.measurement, name)
                    data.append('%s%s value=%s %d' %
                                (n, tags, s, record['dateTime']*1000000000))
                elif self.line_format == 'multi-line':
                    # use multiple lines
                    data.append('%s%s value=%s %d' %
                                (name, tags, s, record['dateTime']*1000000000))
                else:
                    # use a single line
                    data.append('%s=%s' % (name, s))
            except (TypeError, ValueError) as e:
                # FIXME: influx1 does not support NULL.  for influx2, ensure
                # that any None values are retained as NULL.
                logdbg("skipped value '%s': %s" % (record.get(k), e))
        if self.line_format == 'multi-line' or self.line_format == 'multi-line-dotted':
            str_data = '\n'.join(data)
        else:
            str_data = '%s%s %s %d' % (self.measurement, tags, ','.join(data), record['dateTime']*1000000000)
        return str_data, 'application/x-www-form-urlencoded'

# Use this hook to test the uploader:
#   PYTHONPATH=bin python bin/user/influx.py

if __name__ == "__main__":
    import optparse
    import time

    weewx.debug = 2

    usage = """Usage: python -m influx --help
       python -m influx --version
       python -m influx [--server-url=SERVER-URL] 
                        [--user=USER] [--password=PASSWORD]
                        [--admin-user=ADMIN-USER] [--admin-password=ADMIN-PASSWORD]
                        [--database=DBNAME] [--measurement=MEASUREMENT]
                        [--tags=TAGS]"""

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--version', action='store_true',
                      help='Display weewx-influx version')
    parser.add_option('--server-url', default='http://localhost:8086',
                      help="URL for the InfluxDB server. Default is 'http://localhost:8086'",
                      metavar="SERVER-URL")
    parser.add_option('--user', default='weewx',
                      help="User name to be used for data posts. Default is 'weewx'",
                      metavar="USER")
    parser.add_option('--password', default='weewx',
                      help="Password for USER. Default is 'weewx'",
                      metavar="PASSWORD")
    parser.add_option('--admin-user',
                      help="Admin user to be used when creating the database.",
                      metavar="ADMIN-USER")
    parser.add_option('--admin-password',
                      help="Password for ADMIN-USER.",
                      metavar="ADMIN-PASSWORD")
    parser.add_option('--database', default='tester',
                      help="InfluxDB database name. Default is 'tester'",
                      metavar="DBNAME")
    parser.add_option('--measurement', default='record',
                      help="InfluxDB measurement name. Default is 'record'",
                      metavar="MEASUREMENT")
    parser.add_option('--tags', default='station=A,field=C',
                      help="Influxdb tags to be used. Default is 'station=A,field=C'",
                      metavar="TAGS")
    (options, args) = parser.parse_args()

    if options.version:
        print("weewx-influxdb version %s" % VERSION)
        exit(0)

    print("Using server-url of '%s'" % options.server_url)

    queue = queue.Queue()
    t = InfluxThread(queue,
                     manager_dict=None,
                     database=options.database,
                     username=options.user,
                     password=options.password,
                     measurement=options.measurement,
                     tags=options.tags,
                     server_url=options.server_url)
    queue.put({'dateTime': int(time.time() + 0.5),
               'usUnits': weewx.US,
               'outTemp': 32.5,
               'inTemp': 75.8,
               'outHumidity': 24})
    queue.put(None)
    t.run()
