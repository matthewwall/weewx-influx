# $Id: install.py 1481 2016-04-18 09:45:28Z mwall $
# installer for influx
# Copyright 2016 Matthew Wall

from setup import ExtensionInstaller

def loader():
    return InfluxInstaller()

class InfluxInstaller(ExtensionInstaller):
    def __init__(self):
        super(InfluxInstaller, self).__init__(
            version="0.2",
            name='influx',
            description='Upload weather data to Influx.',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            restful_services='user.influx.Influx',
            config={
                'StdRESTful': {
                    'Influx': {
                        'database': 'INSERT_DATABASE_HERE',
                        'host': 'INSERT_HOST_HERE'}}},
            files=[('bin/user', ['bin/user/influx.py'])]
            )
