# installer for influx
# Copyright 2016-2020 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return InfluxInstaller()

class InfluxInstaller(ExtensionInstaller):
    def __init__(self):
        super(InfluxInstaller, self).__init__(
            version="0.20",
            name='influx',
            description='Upload weather data to Influx.',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            restful_services='user.influx.Influx',
            config={
                'StdRESTful': {
                    'Influx': {
                        'server_url': 'http://localhost:8086',
                        'org': 'replace_me',
                        'bucket': 'replace_me',
                        'token': 'replace_me'}}},
            files=[('bin/user', ['bin/user/influx.py'])]
            )
