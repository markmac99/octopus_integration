# Copyright Mark McIntyre, 2024-

# some functions to update various values in my Openhab InfluxDB

import datetime
import os
import pandas as pd
from influxdb import InfluxDBClient
import logging

from updateOpenhab import updateMeterReading
from influxconfig import getInfluxUrl, getMeasurementName

from octopus import getPrice

log = logging.getLogger('octopus_misc')


def updateRecentGasCost(ed=None, lookback=90):
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    if not ed:
        ed = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
    sd = ed - datetime.timedelta(minutes=lookback)
    ts1 = sd.strftime('%Y-%m-%dT%H:00:00Z')
    ts2 = ed.strftime('%Y-%m-%dT%H:%M:%SZ')
    vals = client.query(f"Select value from HouseGasPower where time >= '{ts1}' and time < '{ts2}'")
    df = pd.DataFrame(vals['HouseGasPower'])
    df['prev_time'] = df.time.shift(1)
    df['time'] = pd.to_datetime(df['time'], format='mixed')
    df['prev_time'] = pd.to_datetime(df['prev_time'], format='mixed')
    df.dropna(inplace=True)
    df['cost'] = [getPrice(e, meastype='gas', amt=v)/100*v/1000 for e,s,v in zip(df.time,df.prev_time, df.value)]

    for _,rw in df.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "GasCost","time": f"{tsmeas}","fields": {"value": rw.cost}}]
        client.write_points(jsbody)

    df2 = df.drop(columns=['value', 'prev_time'])
    df2.set_index('time', inplace=True)
    halfhourly = df2.resample("30min").sum()
    halfhourly.reset_index(inplace=True)
    
    for _,rw in halfhourly.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "GasCost30m","time": f"{tsmeas}","fields": {"value": rw.cost}}]
        client.write_points(jsbody)

    updateDailyTotalGasCost(client, ed, daysback=lookback/24/60)

    return 


def updateDailyTotalGasCost(client, ed, daysback=7):
    print(f'looking back {daysback} days')
    sd = ed - datetime.timedelta(days=daysback)
    ts1 = sd.strftime('%Y-%m-%dT00:00:00Z')
    ts2 = ed.strftime('%Y-%m-%dT%H:%M:%SZ')
    vals = client.query(f"Select value from GasCost30m where time >= '{ts1}' and time <= '{ts2}'")
    df = pd.DataFrame(vals['GasCost30m'])
    df.set_index('time', inplace=True)
    df.index = pd.to_datetime(df.index)
    daily = df.resample("1d").sum()
    daily.reset_index(inplace=True)
    for _,rw in daily.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "GasCostDay","time": f"{tsmeas}","fields": {"value": rw.value}}]
        client.write_points(jsbody)


def updateRecentElectricityCost(ed=None, lookback=90, src='Enphase_Envoy_Grid_Watts'):
    """
    Get electricity cost, either from the Enphase gateway or from Octopus' raw data (HouseElectricityPower)
    """
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    if not ed:
        ed = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
    sd = ed - datetime.timedelta(minutes=lookback)
    ts1 = sd.strftime('%Y-%m-%dT%H:00:00Z')
    ts2 = ed.strftime('%Y-%m-%dT%H:%M:%SZ')
    vals = client.query(f"Select value from {src} where time >= '{ts1}' and time < '{ts2}'")
    df = pd.DataFrame(vals[src])
    df['prev_time'] = df.time.shift(1)
    df['time'] = pd.to_datetime(df['time'], format='mixed')
    df['prev_time'] = pd.to_datetime(df['prev_time'], format='mixed')
    df.dropna(inplace=True)
    df['cost'] = [(e-s).seconds/3600*getPrice(e, amt=v)/100*v/1000 for e,s,v in zip(df.time,df.prev_time, df.value)]

    for _,rw in df.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "ElectricityCost","time": f"{tsmeas}","fields": {"value": rw.cost}}]
        client.write_points(jsbody)

    df2 = df.drop(columns=['value', 'prev_time'])
    df2.set_index('time', inplace=True)
    halfhourly = df2.resample("30min").sum()
    halfhourly.reset_index(inplace=True)
    
    for _,rw in halfhourly.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "ElectricityCost30m","time": f"{tsmeas}","fields": {"value": rw.cost}}]
        client.write_points(jsbody)

    updateDailyTotalElecCost(client, ed)

    return 


def updateDailyTotalElecCost(client, ed):
    sd = ed - datetime.timedelta(days=7)
    ts1 = sd.strftime('%Y-%m-%dT00:00:00Z')
    ts2 = ed.strftime('%Y-%m-%dT%H:%M:%SZ')
    vals = client.query(f"Select value from ElectricityCost30m where time >= '{ts1}' and time <= '{ts2}'")
    df = pd.DataFrame(vals['ElectricityCost30m'])
    df.set_index('time', inplace=True)
    df.index = pd.to_datetime(df.index, format='mixed')
    daily = df.resample("1d").sum()
    daily.reset_index(inplace=True)
    for _,rw in daily.iterrows():
        tsmeas = rw.time.strftime('%Y-%m-%dT%H:%M:00Z')
        jsbody = [{"measurement": "ElectricityCostDay","time": f"{tsmeas}","fields": {"value": rw.value}}]
        client.write_points(jsbody)


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
        log.info(f'updating electicity for {ts1} with {usage}')
        for i in range(0,48):
            tsmeas = (sd + datetime.timedelta(minutes = i*30)).strftime('%Y-%m-%dT%H:%M:00Z')
            jsbody = [{"measurement": "DailyHouseEnergy","time": f"{tsmeas}","fields": {"value": usage}}]
            client.write_points(jsbody)
    else:
        log.info(f'No electicity data for {ts1}')


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
        log.info(f'updating gas for {ts1} with {usage}')
        for i in range(0,48):
            tsmeas = (sd + datetime.timedelta(minutes = i*30)).strftime('%Y-%m-%dT%H:%M:00Z')
            jsbody = [{"measurement": "DailyHouseGas","time": f"{tsmeas}","fields": {"value": usage}}]
            client.write_points(jsbody)
    else:
        log.info(f'No gas data for {ts1}')


def updateMeter(elec=True):
    _, usr, pwd, infhost, infport, ohdb = getInfluxUrl()
    client = InfluxDBClient(host=infhost, port=infport, username=usr, password=pwd, database=ohdb)
    currentmeter = 0

    if elec:
        metertbl = 'HouseElectricityMeterReading'
        livetbl = 'OctopusElect30m'
    else:
        metertbl = 'HouseGasMeterReading'
        livetbl = 'OctopusGas30m'

    vals = client.query(f"select last(value)  from {metertbl}")
    if not vals:
        log.warning('unable to get last saved meter value')
        return 
    lasttime = vals.raw['series'][0]['values'][0][0]
    lastval = float(vals.raw['series'][0]['values'][0][1])
    while lastval == 0:
        vals = client.query(f"select last(value)  from {metertbl} where time < '{lasttime}'")
        lasttime = vals.raw['series'][0]['values'][0][0]
        lastval = float(vals.raw['series'][0]['values'][0][1])
    log.info(f'last good value was {lastval} at {lasttime}')
    vals = client.query(f"select time, value from {livetbl} where time >= '{lasttime}'")
    if not vals:
        log.warning('no new consumption data')
        return 
    wattsadded = 0
    for rw in vals.raw['series'][0]['values']:
            wattsadded += float(rw[1])
            ed = rw[0]
    log.info(f'added {wattsadded} since {lasttime}')   
    if elec:
        currentmeter = lastval + wattsadded/1000.0
    else:
        # convert watts to cubic metres
        # cum x 1.02264 x 40.0 ÷ 3.6 = kWh
        cumadded = wattsadded/1000.0 / (1.02264 * 40.0 /3.6)
        currentmeter = lastval + cumadded
    
    if currentmeter > 0:
        jsbody = [{"measurement": metertbl,"time": f"{ed}",
                   "fields": {"value": round(currentmeter,1)}}]
        client.write_points(jsbody)
        log.info(f'updated {metertbl} to {currentmeter} as of {ed}')
        try:
            ts = datetime.datetime.strptime(ed, '%Y-%m-%dT%H:%M:%SZ')
        except:
            ts = datetime.datetime.strptime(ed, '%Y-%m-%dT%H:%M:%S.%fZ')
        updateMeterReading(currentmeter, ts, elec)
    return 


if __name__ == '__main__': 
    logpath = os.path.expanduser('~/logs')
    logname=os.path.join(logpath, 'octopus_misc.log')
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(logname, 'a+')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(fh)

    log.info('Starting')
    updateDailyHouseElectricity(datetime.datetime.now())
    updateDailyHouseElectricity(datetime.datetime.now() + datetime.timedelta(days=-1))
    updateDailyHouseElectricity(datetime.datetime.now() + datetime.timedelta(days=-2))

    updateDailyHouseGas(datetime.datetime.now())
    updateDailyHouseGas(datetime.datetime.now() + datetime.timedelta(days=-1))
    updateDailyHouseGas(datetime.datetime.now() + datetime.timedelta(days=-2))

    updateMeter(elec=True)
    updateMeter(elec=False)
    log.info('Finished')
