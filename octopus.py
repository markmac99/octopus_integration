# -*- coding: utf-8 -*-
"""
Script to intergrate octopus data the Openhab
The API does not provide realtime data so we can't push directly to openhab. Instead we need to push to InfluxDB
using the timestamps of the data, and hope that OH understands whats going on. 

"""
import os
import requests
from requests.auth import HTTPBasicAuth 
import datetime
import pandas as pd

from loadconfig import getApiKey, getAccountId, getDataDir
from influxconfig import getInfluxUrl, getMeasurementName

ACCTDETS_URL = 'https://api.octopus.energy/v1/accounts'
ELEC_URL = 'https://api.octopus.energy/v1/electricity-meter-points'
GAS_URL = 'https://api.octopus.energy/v1/gas-meter-points'

CAL_VAL = 39.2
VOL_CORR = 1.02264
JOULE_CORR = 3.6


def getOctopusMeters():
    # assumes only one property and one meter per property
    url = f'{ACCTDETS_URL}/{getAccountId()}/'
    try:
        r = requests.get(url, auth=(getApiKey(),''))
        data = r.json()
        mpan = data['properties'][0]['electricity_meter_points'][0]['mpan']    
        esns = [x['serial_number'] for x in data['properties'][0]['electricity_meter_points'][0]['meters']]
        mprn = data['properties'][0]['gas_meter_points'][0]['mprn']    
        gsns = [x['serial_number'] for x in data['properties'][0]['gas_meter_points'][0]['meters']]
        return mpan, esns, mprn, gsns 
    except Exception:
        print('failed to get meter ids')
        return False, 0, 0, 0
    

def saveAsCsv(thisdf, typ, outdir):
    # convert data to time-indexed dataframe
    yr = datetime.datetime.utcnow().year
    thisdf.set_index('timestamp', inplace=True)
    thisdf.drop(columns=['interval_start','interval_end'], inplace=True)
    #print(df)
    # load existing data, if any
    dataf = os.path.join(outdir, f'{typ}-{yr}.csv')
    if os.path.isfile(dataf):
        currdf = pd.read_csv(dataf)
        currdf['ts2'] = [datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z') for x in currdf.timestamp]
        currdf.drop(columns=['timestamp'], inplace=True)
        currdf.rename(columns={'ts2':'timestamp'}, inplace=True)
        currdf.set_index('timestamp', inplace=True)
        thisdf = pd.concat([thisdf,currdf])
        thisdf = thisdf[~thisdf.index.duplicated(keep='first')]
    thisdf.sort_index(inplace=True)
    thisdf.to_csv(dataf)
    print(f' saved {typ} - latest value is { thisdf.iloc[-1].consumption} at {thisdf.iloc[-1].name}')
    return


def getOneDataset(meterid, serialno, elecdata=True, daysback=14):
    p2 = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    p1 = (datetime.datetime.utcnow() + datetime.timedelta(days=-daysback)).strftime('%Y-%m-%dT%H:%M:%SZ')
    datadf = None
    if elecdata:
        urlroot = ELEC_URL
    else:
        urlroot = GAS_URL
    url = f'{urlroot}/{meterid}/meters/{serialno}/consumption/?page_size=1000&period_from={p1}&period_to={p2}&order_by=period'
    try:
        r = requests.get(url, auth=(getApiKey(),''))
        data = r.json()
        if 'count' in data:
            if data['count'] > 0:
                datadf = pd.DataFrame(data['results'])
                datadf['timestamp'] = [datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z') for x in datadf.interval_end]
                print(f'got data for {serialno}')
    except Exception:
        print(f'unable to get data from {urlroot}')
    return datadf


def getDataFromOctopus(mpan, esns, mprn, gsns):
    for esn in esns:
        datadf = getOneDataset(mpan, esn, True)
        if datadf is not None: 
            saveAsCsv(datadf.copy(True), 'electricity', getDataDir())
            updateInfluxDB(datadf.copy(True), 'electricity', getDataDir())
    for gsn in gsns:
        datadf = getOneDataset(mprn, gsn, False)
        if datadf is not None: 
            saveAsCsv(datadf.copy(True), 'gas', getDataDir())
            updateInfluxDB(datadf.copy(True), 'gas', getDataDir())
    return 


def convertToIdbFmt(df, meas, outdir):
    df['measurement'] = [meas]*len(df)
    df['ts'] = [int(x.timestamp()*1e9) for x in df.timestamp]
    df.drop(columns=['interval_start','interval_end', 'timestamp'], inplace=True)
    with open(os.path.join(outdir, f'{meas}.txt'), 'w', newline='\n') as outf:
        for _, rw in df.iterrows():
            if 'Gas' in meas:
                cons = round(rw.consumption * 1000 * CAL_VAL * VOL_CORR /JOULE_CORR, 1)
            else:
                cons = rw.consumption * 1000
            outf.write(f'{rw.measurement} value={cons} {rw.ts}\n')
    idbdata = open(os.path.join(outdir, f'{meas}.txt'), 'rb').read()
    return idbdata
    

def updateInfluxDB(df, typ, outdir):
    url, usr, pwd, _, _, _ = getInfluxUrl()
    meas = getMeasurementName(typ)
    idbdata = convertToIdbFmt(df, meas, outdir=outdir)
    # curl -i -XPOST -u $influxuser:$influxpw "http://$influxserver:$influxport/write?db=$influxdatbase" --data-binary @$i
    authn = HTTPBasicAuth(usr, pwd)
    requests.post(url, data=idbdata, auth=authn)
    return len(df)


if __name__ == '__main__': 
    mpan, esns, mprn, gsns = getOctopusMeters()
    if mpan:
        getDataFromOctopus(mpan, esns, mprn, gsns)
