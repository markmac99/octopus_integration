# Copyright Mark McIntyre, 2024-

# some functions to update various values in my Openhab InfluxDB

import datetime
from influxconfig import getInfluxUrl
from influxdb import InfluxDBClient
from updateOpenhab import updateMeterReading

from influxconfig import getMeasurementName


def updateDailyHouseElectricity(sd):
    meas = getMeasurementName('electricity')
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)

    ts1 = sd.strftime('%Y-%m-%dT00:00:00Z')
    sd = datetime.datetime.strptime(ts1,'%Y-%m-%dT00:00:00Z')
    ed = sd + datetime.timedelta(days=1)
    ts2 = ed.strftime('%Y-%m-%dT00:00:00Z')
    vals = client.query(f"Select sum(value) from {meas} where time >= '{ts1}' and time < '{ts2}'")
    if vals:
        usage = vals.raw['series'][0]['values'][0][1]/1000
        print(f'updating electicity for {ts1} with {usage}')
        for i in range(0,48):
            tsmeas = (sd + datetime.timedelta(minutes = i*30)).strftime('%Y-%m-%dT%H:%M:00Z')
            jsbody = [{"measurement": "DailyHouseEnergy","time": f"{tsmeas}","fields": {"value": usage}}]
            client.write_points(jsbody)
    else:
        print(f'No electicity data for {ts1}')


def updateDailyHouseGas(sd):
    meas = getMeasurementName('gas')
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)

    ts1 = sd.strftime('%Y-%m-%dT00:00:00Z')
    sd = datetime.datetime.strptime(ts1,'%Y-%m-%dT00:00:00Z')
    ed = sd + datetime.timedelta(days=1)
    ts2 = ed.strftime('%Y-%m-%dT00:00:00Z')
    vals = client.query(f"Select sum(value) from {meas} where time >= '{ts1}' and time < '{ts2}'")
    if vals:
        usage = vals.raw['series'][0]['values'][0][1]/1000
        print(f'updating gas for {ts1} with {usage}')
        for i in range(0,48):
            tsmeas = (sd + datetime.timedelta(minutes = i*30)).strftime('%Y-%m-%dT%H:%M:00Z')
            jsbody = [{"measurement": "DailyHouseGas","time": f"{tsmeas}","fields": {"value": usage}}]
            client.write_points(jsbody)
    else:
        print(f'No gas data for {ts1}')


def updateElecMeter(sd=None):
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    currentmeter = 0
    if sd is None:
        sd = datetime.datetime.now().replace(hour=1, minute=0,second=0, microsecond=0) + datetime.timedelta(days=-3)
    maxdt = datetime.datetime.now().replace(hour=1, minute=0,second=0, microsecond=0)
    while sd < maxdt:
        ts1 = sd.strftime('%Y-%m-%dT%H:%M:00Z')
        ed = sd + datetime.timedelta(minutes=30)
        ts2 = ed.strftime('%Y-%m-%dT%H:%M:00Z')
        vals = client.query(f"select * from HouseElectricityPower where time >= '{ts1}' and time < '{ts2}'")
        if vals:
            usage = vals.raw['series'][0]['values'][0][1]/1000
        else:
            usage = 0
        vals = client.query(f"select * from HouseElectricityMeterReading where time >= '{ts1}' and time < '{ts2}'")
        if vals:
            currvals = vals.raw['series'][-1]
            validx = currvals['columns'].index('value')
            currentmeter = currvals['values'][0][validx]
            currentmeter += usage
        #print(f'updating elemeter for {ts1} with {currentmeter}')
        if currentmeter > 0:
            jsbody = [{"measurement": "HouseElectricityMeterReading","time": f"{ts2}","fields": {"value": round(currentmeter,1)}}]
            client.write_points(jsbody)
        sd = ed
    print(f'updated electricity meter to {currentmeter} as of {ed}')
    updateMeterReading(currentmeter, ed, True)


if __name__ == '__main__': 
    updateDailyHouseElectricity(datetime.datetime.now())
    updateDailyHouseElectricity(datetime.datetime.now() + datetime.timedelta(days=-1))
    updateDailyHouseElectricity(datetime.datetime.now() + datetime.timedelta(days=-2))

    updateDailyHouseGas(datetime.datetime.now())
    updateDailyHouseGas(datetime.datetime.now() + datetime.timedelta(days=-1))
    updateDailyHouseGas(datetime.datetime.now() + datetime.timedelta(days=-2))

    #updateElecMeter()
