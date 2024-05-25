# copyright mark mcintyre 2024-

# get the influxdb settings

import os


def getInfluxUrl():
    passwordfile = os.path.expanduser('~/.ssh/influxdb')
    with open(passwordfile) as inf:
        usr = inf.readline().strip()
        pwd = inf.readline().strip()    
    influxserver = 'wxsatpi'
    influxport = 8086
    influxdatabase = 'openhab'
    influxurl = f'http://{influxserver}:{influxport}/write?db={influxdatabase}'
    return influxurl, usr, pwd, influxserver, influxport, influxdatabase


def getMeasurementName(typ):
    if typ == 'electricity':
        return 'HouseElectricityPower'
    elif typ == 'gas':
        return 'HouseGasPower'
    else:
        return None
