influx - weewx extension that sends data to an influx data server
Copyright 2016-2021 Matthew Wall
Distributed under terms of the GPLv3

===============================================================================
Installation

1) download

wget -O weewx-influx.zip https://github.com/matthewwall/weewx-influx/archive/master.zip

2) run the installer:

wee_extension --install weewx-influx.zip

3) enter parameters in weewx.conf:

[StdRESTful]
    [[Influx]]
        host = HOSTNAME
        database = DATABASE

4) restart weewx:

sudo /etc/init.d/weewx stop
sudo /etc/init.d/weewx start


===============================================================================
Configuration

A minimal configuration requires only a host and database.

Use host and port, or server_url to specify the influx server.

A username and password are required if authentication is enabled on the
influx server.

When it starts up, this extension will attempt to create the influx database.
If credentials for a database administrator were provided, it will use those
credentials.  Otherwise, it will use the username/password credentials.

Here is a complete enumeration of options.  Specify only those that you need.

[StdRESTful]
    [[Influx]]
        database = DATABASE
        port = 8086
        host = example.com                         # specify host OR server_url
        server_url = https://example.com:443       # specify server_url OR host
        username = USERNAME
        password = PASSWORD
        dbadmin_username = DATABASE_ADMINISTRATOR_USERNAME
        dbadmin_password = DATABASE_ADMINISTRATOR_PASSWORD
        binding = (loop | archive)                 # default is archive
        measurement = label                        # default is record
        tags = station=A,field=C                   # optional
        line_format = (multi-line | single-line)   # default is single-line
        obs_to_upload = (all | most | none)        # default is most
        append_units_label = (True | False)        # default is true
        unit_system = (US | METRIC | METRICWX)     # default is database system
        augment_record = (True | False)            # default is true
        [[[inputs]]]                               # optional
            [[[[observation1]]]]
                units = degree_F                   # optional for each obs
                name = label                       # optional for each obs
                format = %.2f                      # optional for each obs
            [[[[observation2]]]]
                units = degree_F                   # optional for each obs
                name = label                       # optional for each obs
                format = %.2f                      # optional for each obs


===============================================================================
Line formats

Influx defines two line formats, multi-line and single-line.  This extension
defines the following formats: single-line, multi-line, and multi-line-dotted.
These correspond to the influx line formats.  The 'measurement' parameter is
used to identify the values sent to influx.

[StdRESTful]
    [[Influx]]
        measurement = weewx
        line_format = multi-line

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


===============================================================================
Input map

When an input map is specified, only variables in that map will be uploaded.
The 'units' parameter can be used to specify which units should be used for
the input, independent of the local weewx units.

[StdRESTful]
    [[Influx]]
        database = DATABASE
        server = localhost
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
