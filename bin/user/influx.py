# Copyright 2016 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

"""
Influx is a platform for collecting, storing, and managing time-series data.

http://influxdata.com

This is a weewx extension that uploads data to an Influx server.

Database Configuration and Access

When it starts up, this extension will attempt to create the influx database.
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

Customized Configuration

This uploader supports two different formats for the influx line protocol.
The first form is multi-line:

name0[tags] value=x ts
name1[tags] value=y ts
name2[tags] value=z ts
...

The second form is single-line:

record[tags] name0=x,name1=y,name2=z ts

[StdRESTful]
    [[Influx]]
        line_format = multi-line # options are multi-line or single-line

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

import Queue
import base64
from distutils.version import StrictVersion
import sys
import syslog
import urllib
import urllib2

import weewx
import weewx.restx
import weewx.units
from weeutil.weeutil import to_bool, accumulateLeaves

VERSION = "0.8"

REQUIRED_WEEWX = "3.5.0"
if StrictVersion(weewx.__version__) < StrictVersion(REQUIRED_WEEWX):
    raise weewx.UnsupportedFeature("weewx %s or greater is required, found %s"
                                   % (REQUIRED_WEEWX, weewx.__version__))

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
    'meter_per_second': 'mps',
    'meter_per_second2': 'mps',
    'degree_compass': None,
    'watt_per_meter_squared': 'Wpm2',
    'uv_index': None,
    'percent': None,
    'unix_epoch': None,
}

# return the units label for an observation
def _get_units_label(obs, unit_system):
    (unit_type, _) = weewx.units.getStandardUnitType(unit_system, obs)
    return UNIT_REDUCTIONS.get(unit_type, unit_type)

# get the template for an observation based on the observation key
def _get_template(obs_key, overrides, append_units_label, unit_system):
    tmpl_dict = dict()
    if append_units_label:
        label = _get_units_label(obs_key, unit_system)
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
        
        tags: name-value pairs to identify the measurement
        Default is None

        line_format: which line protocol format to use.  Possible values are
        single-line or multi-line.
        Default is single-line

        append_units_label: should units label be appended to name
        Default is True

        obs_to_upload: Which observations to upload.  Possible values are
        none or all.  When none is specified, only items in the inputs list
        will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is all

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None

        binding: either loop or archive
        Default is archive
        """
        super(Influx, self).__init__(engine, cfg_dict)        
        loginf("service version is %s" % VERSION)
        try:
            site_dict = cfg_dict['StdRESTful']['Influx']
            site_dict = accumulateLeaves(site_dict, max_level=1)
            site_dict['database']
        except KeyError, e:
            logerr("Data will not be uploaded: Missing option %s" % e)
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
        site_dict.setdefault('tags', None)
        site_dict.setdefault('line_format', 'single-line')
        site_dict.setdefault('obs_to_upload', 'all')
        site_dict.setdefault('append_units_label', True)
        site_dict.setdefault('augment_record', True)

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
            loginf("tags %s" % site_dict['tags'])
        loginf("database is %s" % site_dict['database'])

        # we can bind to either loop packets or archive records
        binding = site_dict.pop('binding', 'archive')
        loginf('binding is %s' % binding)

        data_queue = Queue.Queue()
        try:
            data_thread = InfluxThread(self.archive_queue, **site_dict)
        except weewx.ViolatedPrecondition, e:
            loginf("Data will not be posted: %s" % e)
            return
        data_thread.start()

        if binding.lower() == 'loop':
            self.archive_queue = data_queue
            self.archive_thread = data_thread
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        else:
            self.loop_queue = data_queue
            self.loop_thread = data_thread
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        loginf("Data will be uploaded to %s" % site_dict['server_url'])

    def new_loop_packet(self, event):
        self.loop_queue.put(event.packet)

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)


class InfluxThread(weewx.restx.RESTThread):

    _DEFAULT_SERVER_URL = 'http://localhost:8086'

    def __init__(self, queue, database,
                 username=None, password=None,
                 dbadmin_username=None, dbadmin_password=None,
                 line_format='single-line', tags=None,
                 unit_system=None, augment_record=True,
                 inputs=dict(), obs_to_upload='all', append_units_label=True,
                 server_url=_DEFAULT_SERVER_URL, skip_upload=False,
                 manager_dict=None,
                 post_interval=None, max_backlog=sys.maxint, stale=None,
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
        self.tags = tags
        self.upload_all = True if obs_to_upload.lower() == 'all' else False
        self.append_units_label = append_units_label
        self.inputs = inputs
        self.server_url = server_url
        self.skip_upload = to_bool(skip_upload)
        self.unit_system = unit_system
        self.augment_record = augment_record
        self.templates = dict()
        self.line_format = line_format

        # ensure that the database exists
        qstr = urllib.urlencode({'q': 'CREATE DATABASE %s' % self.database})
        url = '%s/query?%s' % (self.server_url, qstr)
        req = urllib2.Request(url)
        req.add_header("User-Agent", "weewx/%s" % weewx.__version__)
        uname = None
        pword = None
        if dbadmin_username is not None:
            uname = dbadmin_username
            pword = dbadmin_password
        elif username is not None:
            uname = username
            pword = password
        if uname is not None:
            b64s = base64.encodestring(
                '%s:%s' % (uname, pword)).replace('\n', '')
            req.add_header("Authorization", "Basic %s" % b64s)
        try:
            self.post_request(req)
        except (urllib2.URLError, socket.error, httplib.BadStatusLine, httplib.IncompleteRead), e:
            logerr("create database failed: %s" % e)

    def process_record(self, record, dbm):
        if self.augment_record and dbm:
            record = self.get_record(record, dbm)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        url = '%s/write?db=%s' % (self.server_url, self.database)
        data = self.get_data(record)
        if weewx.debug >= 2:
            logdbg('url: %s' % url)
            logdbg('data: %s' % data)
        if self.skip_upload:
            raise AbortedPost()
        req = urllib2.Request(url, data)
        req.add_header("User-Agent", "weewx/%s" % weewx.__version__)
        if self.username is not None:
            b64s = base64.encodestring(
                '%s:%s' % (self.username, self.password)).replace('\n', '')
            req.add_header("Authorization", "Basic %s" % b64s)
        req.get_method = lambda: 'POST'
        self.post_with_retries(req)

    def check_response(self, response):
        if response.code == 204:
            return
        payload = response.read()
        if payload and payload.find('results') >= 0:
            logdbg("code: %s payload: %s" % (response.code, payload))
            return
        raise weewx.restx.FailedPost("Server returned '%s' (%s)" %
                                     (response.read(), response.code))

    def handle_exception(self, e, count):
        if isinstance(e, urllib2.HTTPError):
            payload = e.read()
            logdbg("exception: %s payload: %s" % (e, payload))
            if payload and payload.find("error") >= 0:
                if payload.find("database not found") >= 0:
                    raise weewx.restx.AbortedPost(payload)
        super(InfluxThread, self).handle_exception(e, count)

    def post_request(self, request, payload=None):
        # FIXME: provide full set of ssl options instead of this hack
        if self.server_url.startswith('https'):
            import ssl
            return urllib2.urlopen(request, data=payload, timeout=self.timeout,
                                   context=ssl._create_unverified_context())
        return urllib2.urlopen(request, data=payload, timeout=self.timeout)

#    def post_request(self, request, payload=None):  # @UnusedVariable
#        try:
#            try:
#                _response = urllib2.urlopen(request, timeout=self.timeout)
#            except TypeError:
#                _response = urllib2.urlopen(request)
#        except urllib2.HTTPError, e:
#            logerr("post failed: %s" % e)
#            raise weewx.restx.FailedPost(e)
#        else:
#            return _response

    def get_data(self, record):
        tags = ''
        if self.tags:
            tags = ',%s' % self.tags

        # if uploading everything, we must check the upload variables list
        # every time since variables may come and go in a record.  use the
        # inputs to override any generic template generation.
        if self.upload_all:
            for f in record:
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
                                (name, tags, s, record['dateTime']*1000000))
                else:
                    # use a single line
                    data.append('%s=%s' % (name, s))
            except (TypeError, ValueError):
                pass
        if self.line_format == 'multi-line':
            return '\n'.join(data)
        return 'record%s %s %d' % (
            tags, ','.join(data), record['dateTime']*1000000000)

# Use this hook to test the uploader:
#   PYTHONPATH=bin python bin/user/influx.py

if __name__ == "__main__":
    import time
    weewx.debug = 2
    queue = Queue.Queue()
    t = InfluxThread(queue,
                     manager_dict=None,
                     database='tester',
                     tags='station=A,field=C',
                     server_url='http://192.168.101.39:8086')
    t.process_record({'dateTime': int(time.time() + 0.5),
                      'usUnits': weewx.US,
                      'outTemp': 32.5,
                      'inTemp': 75.8,
                      'outHumidity': 24}, None)
