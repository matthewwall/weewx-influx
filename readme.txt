influx - weewx extension that sends data to an influx data server
Copyright 2016 Matthew Wall

Installation instructions:

1) run the installer:

wee_extension --install weewx-influx.tgz

2) enter parameters in weewx.conf:

[StdRESTful]
    [[Influx]]
        host = HOSTNAME
        database = DATABASE

3) restart weewx:

sudo /etc/init.d/weewx stop
sudo /etc/init.d/weewx start
