# copyright mark mcintyre 2024-

# Python code to retrieve solar estimates from forecast.solar

import os
import requests
from requests.auth import HTTPBasicAuth 
import datetime
import pandas as pd
import logging

from influxconfig import getInfluxUrl
from loadconfig import getDataDir, getSolarApiKey, getLocation, getArrayDetails

log = logging.getLogger('solar')


def getForecastData():
    apikey = getSolarApiKey()
    lati, longi = getLocation()
    decls, azims, kwps = getArrayDetails()

    log.info('Getting data from forecast.solar')
    fulldf = None
    for reqval in ['watthours', 'watts']:
        df = None
        for i in range(0, len(decls), 2):
            if i+1 < len(decls):
                apiurl = f'https://api.forecast.solar/{apikey}/estimate/{reqval}/{lati}/{longi}/{decls[i]}/{azims[i]}/{kwps[i]}/{decls[i+1]}/{azims[i+1]}/{kwps[i+1]}'
            else:
                apiurl = f'https://api.forecast.solar/{apikey}/estimate/{reqval}/{lati}/{longi}/{decls[i]}/{azims[i]}/{kwps[i]}'
            r = requests.get(apiurl)
            if not r.ok:
                return None
            data = r.json()
            thisdf = pd.DataFrame([data['result']]).transpose()
            thisdf.rename(columns={0:reqval}, inplace=True)
            if df is None:
                df = thisdf
            else:
                df = thisdf.add(df, fill_value=0)
        if fulldf is None:
            fulldf = df
        else:
            fulldf = df.merge(fulldf, left_index=True, right_index=True)
    log.info('Retrieved')
    return fulldf


def saveAsCsv(thisdf, outdir):
    # convert data to time-indexed dataframe
    yr = datetime.datetime.now(datetime.timezone.utc).year
    # load existing data, if any
    dataf = os.path.join(outdir, f'solar-{yr}.csv')
    if os.path.isfile(dataf):
        currdf = pd.read_csv(dataf)
        currdf.rename(columns={'Unnamed: 0':'timestamp'}, inplace=True)
        currdf.set_index('timestamp', inplace=True)
        thisdf = pd.concat([thisdf,currdf])
        thisdf = thisdf[~thisdf.index.duplicated(keep='first')]
    thisdf.sort_index(inplace=True)
    thisdf.to_csv(dataf)
    log.info('got solar data')
    return


def convertToIdbFmt(df, meas, outdir):
    log.info('converting to Influx format')
    df['measurement'] = [f'solar_{meas}']*len(df)
    df['ts'] = [int(datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timestamp()*1e9) for x in df.index]
    with open(os.path.join(outdir, f'solar_{meas}.txt'), 'w', newline='\n') as outf:
        for _, rw in df.iterrows():
            cons = rw[meas]
            if meas == 'watthours':
                cons = cons/1000
            outf.write(f'{rw.measurement} value={cons} {rw.ts}\n')
    idbdata = open(os.path.join(outdir, f'solar_{meas}.txt'), 'rb').read()
    return idbdata
    

def updateInfluxDB(df, outdir):
    log.info('Updating influx DB')
    url, usr, pwd, _, _, _ = getInfluxUrl()
    for meas in ['watts', 'watthours']:
        idbdata = convertToIdbFmt(df, meas, outdir=outdir)
        # curl -i -XPOST -u $influxuser:$influxpw "http://$influxserver:$influxport/write?db=$influxdatbase" --data-binary @$i
        authn = HTTPBasicAuth(usr, pwd)
        requests.post(url, data=idbdata, auth=authn)
    return len(df)


if __name__ == '__main__':
    datadir = getDataDir()
    os.makedirs(datadir, exist_ok=True)
    logpath = os.path.expanduser('~/logs')
    os.makedirs(logpath, exist_ok=True)
    logname=os.path.join(logpath, 'forecast-solar.log')    
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(logname, 'a+')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(fh)

    log.info('Starting data capture')
    dta = getForecastData()
    if dta is not None:
        saveAsCsv(dta.copy(True), datadir)
        updateInfluxDB(dta.copy(True), datadir)
        log.info('done')
    else:
        log.warning('unable to retrieve data, trying again later')
