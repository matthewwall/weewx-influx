# Copyright 2016-2020 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

"""
Influx is a platform for collecting, storing, and managing time-series data.

http://influxdata.com

This is a weewx extension that uploads data to an Influx server.

Database Configuration and Access

When it starts up, this extension will attempt to create the influx bucket.

To disable bucket creation, set create_bucket=False.

If credentials for a database administrator were provided, it will use those
credentials.  Otherwise, it will use the username/password credentials.

All other access to the influx database uses the username/password credentials
(non database administrator), if provided.

Minimal Configuration

Org, bucket and token are required.  All weewx variables will be uploaded using
weewx names and default units and formatting.

[StdRESTful]
    [[Influx]]
        server_url = http://localhost:8086
        org = ORG_NAME
        bucket = BUCKET
        token = TOKEN

Customization: line format and database structure

This uploader supports two different formats for the influx line protocol.

[StdRESTful]
    [[Influx]]
        line_format = multi-line # options are multi-line or single-line

The single-line format results in the following:

  record[tags] name0=x,name1=y,name2=z ts

The multi-line format results in the following:

  name0[tags] value=x ts
  name1[tags] value=y ts
  name2[tags] value=z ts
  ...

Which format should you use?  It depends on how you want the data to end up in
influx.  For influx, think of measurement name as table, tags and column names,
and fields as unindexed columns.

For example, consider these data points:

{'H19': 528, 'VPV': 63.68, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.41, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.68, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 14.43, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.71, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.43, 'CS': 5, 'PPV': 9}
{'H19': 528, 'VPV': 63.74, 'I': 600, 'H21': 115, 'H20': 19, 'H23': 93, 'H22': 23, 'V': 13.43, 'CS': 5, 'PPV': 9}

A single-line configuration:

Results in this:

> select * from value
name: value
time                CS H19 H20 H21 H22 H23 I   PPV V     VPV   binding
----                -- --- --- --- --- --- -   --- -     ---   -------
1536086335000000000 5  528 19  115 23  93  0.6 9   13.41 63.68 loop   
1536086337000000000 5  528 19  115 23  93  0.6 9   13.43 63.68 loop   
1536086339000000000 5  528 19  115 23  93  0.6 9   13.43 63.71 loop   
1536086341000000000 5  528 19  115 23  93  0.6 9   13.43 63.74 loop   

A multi-line configuration:

Results in this:

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
        server_url = http://localhost:8086
        org = ORG_NAME
        bucket = BUCKET
        token = TOKEN
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

VERSION = "0.20"

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
        syslog.syslog(level, 'restx: Influx: %s:' % msg)

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

        org: InfluxDB 2.x Organization Name

        bucket: bucket to write data to

        token: InfluxDB 2.x Authorization Token

        Optional parameters:

        server_url: InfluxDB Server URL
        Default is http://localhost:8086

        measurement: name of the measurement
        Default is 'record'

        tags: comma-delimited list of name=value pairs to identify the
        measurement.  tags cannot contain spaces.
        Default is None

        loop_bucket: bucket to write loop records to, if different than
        archive records.
        Default is the 'bucket' parameter

        line_format: which line protocol format to use.  Possible values are
        single-line or multi-line.
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

        add_binding_tag: Whether or not to include the binding (archive
        or loop) as a tag for each data point.
        Default is True
        """
        super(Influx, self).__init__(engine, cfg_dict)
        loginf("service version is %s" % VERSION)
        site_dict = weewx.restx.get_site_dict(cfg_dict, 'Influx', 'org', 'bucket', 'token')
        if site_dict is None:
            return

        loginf("Org is %s" % site_dict['org'])
        loginf("Bucket is %s" % site_dict['bucket'])
        if site_dict.get('loop_bucket') is not None:
            loginf("Loop Bucket is %s" % site_dict['loop_bucket'])

        site_dict.setdefault('server_url', 'http://localhost:8086')
        site_dict.setdefault('loop_bucket', site_dict['bucket'])
        site_dict.setdefault('tags', None)
        site_dict.setdefault('line_format', 'single-line')
        site_dict.setdefault('obs_to_upload', 'most')
        site_dict.setdefault('append_units_label', True)
        site_dict.setdefault('augment_record', True)
        site_dict.setdefault('measurement', 'record')
        site_dict.setdefault('add_binding_tag', True)

        site_dict['append_units_label'] = to_bool(
            site_dict.get('append_units_label'))
        site_dict['augment_record'] = to_bool(site_dict.get('augment_record'))

        usn = site_dict.get('unit_system', None)
        if usn in weewx.units.unit_constants:
            site_dict['unit_system'] = weewx.units.unit_constants[usn]
            loginf("desired unit system is %s" % usn)

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
            loginf("tags %s" % site_dict['tags'])

        # we can bind to loop packets and/or archive records
        binding = site_dict.pop('binding', 'archive')
        if isinstance(binding, list):
            binding = ','.join(binding)
        loginf('binding is %s' % binding)

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

    def __init__(self, queue, server_url, org, bucket, token,
                 loop_bucket=None,
                 line_format='single-line',
                 measurement='record', tags=None,
                 unit_system=None, augment_record=True,
                 inputs=dict(), obs_to_upload='most', append_units_label=True,
                 add_binding_tag=True,
                 skip_upload=False,
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
        self.org = org
        self.bucket = bucket
        self.token = token
        self.loop_bucket = loop_bucket
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
        self.add_binding_tag = to_bool(add_binding_tag)

    def get_record(self, record, dbm):
        # We allow the superclass to add stuff to the record only if the user
        # requests it
        if self.augment_record and dbm:
            record = super(InfluxThread, self).get_record(record, dbm)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        return record

    def format_url(self, record):
        bucket = self.loop_bucket if record.get('binding', None) == "loop" else self.bucket
        return '%s/api/v2/write?%s' % (self.server_url, urlencode({'org': self.org, 'bucket': bucket}))

    def get_request(self, url):
        """Override and add Authorization Token"""

        # Get the basic Request from my superclass
        request = super(InfluxThread, self).get_request(url)
        request.add_header("Authorization", "Token %s" % self.token)

        return request

    def check_response(self, response):
        # TODO: Update for InfluxDB v2
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
            payload = e.read.decode()
            logdbg("exception: %s payload: %s" % (e, payload))

            # Influx DB v2 returns 401 for an invalid token and 403 for insufficient
            # permission, in either case, a simply retry is unlikey to succeed.
            if e.code == 401 or e.code == 403:
                raise weewx.restx.BadLogin(payload)

            # A 404 is returned if the Org or Bucket doesn't exist
            if e.code == 404:
                raise weewx.restx.AbortedPost(payload)
        super(InfluxThread, self).handle_exception(e, count)

    def post_request(self, request, payload=None):
        # FIXME: provide full set of ssl options instead of this hack
        if self.server_url.startswith('https'):
            import ssl
            return urlopen(request, data=payload, timeout=self.timeout,
                           context=ssl._create_unverified_context())
        else:
            return super(InfluxThread, self).post_request(request, payload)

    def get_post_body(self, record):
        """Override my superclass and get the body of the POST"""

        # create the list of tags
        tags = ''
        binding = record.pop('binding', None)
        loginf("Add Bindding Tag = %s" % self.add_binding_tag)
        if binding is not None and self.add_binding_tag:
            loginf("Adding Binding Tag")
            tags = ',binding=%s' % binding
        if self.tags:
            tags = '%s,%s' % (tags, self.tags)
        loginf("tags = %s" % tags)

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
                if self.line_format == 'multi-line':
                    # use multiple lines
                    data.append('%s%s value=%s %d' %
                                (name, tags, s, record['dateTime']*1000000000))
                else:
                    # use a single line
                    data.append('%s=%s' % (name, s))
            except (TypeError, ValueError):
                pass
        if self.line_format == 'multi-line':
            str_data = '\n'.join(data)
        else:
            str_data = '%s%s %s %d' % (self.measurement, tags, ','.join(data), record['dateTime']*1000000000)

        return str_data, 'text/plain'

# Use this hook to test the uploader:
#   PYTHONPATH=bin python bin/user/influx.py

if __name__ == "__main__":
    import optparse
    import time
    import os

    weewx.debug = 2

    usage = """Usage: python -m influx --help
       python -m influx --version
       python -m influx [--server-url=SERVER-URL] 
                        [--org=ORG] [--bucket=BUCKET] [--token=TOKEN]
                        [--measurement=MEASUREMENT]
                        [--tags=TAGS]"""

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--version', action='store_true',
                      help='Display weewx-influx version')
    parser.add_option('--server-url', default=os.environ['INFLUX_HOST'],
                      help="URL for the InfluxDB server. Default is $INFLUX_HOST",
                      metavar="SERVER-URL")
    parser.add_option('--org', default=os.environ['INFLUX_ORG'],
                      help="InfluxDB Organization. Default is $INFLUX_ORG",
                      metavar="ORG_NAME")
    parser.add_option('--token', default=os.environ['INFLUX_TOKEN'],
                      help="InluxDfB Authorization Token. Default is $INFLUX_TOKEN",
                      metavar="TOKEN")
    parser.add_option('--bucket', default="WeeWXTest",
                      help="InluxDfB Bucket, default is 'WeeWXTest'",
                      metavar="BUCKET")
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
                     org=options.org,
                     bucket=options.bucket,
                     token=options.token,
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
