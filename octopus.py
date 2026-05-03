# -*- coding: utf-8 -*-
"""
Script to integrate octopus data with Openhab
The API does not provide realtime data so we can't push directly to openhab. Instead we need to push to InfluxDB
using the timestamps of the data, and hope that OH understands whats going on. 

Important Note: octopus energy data is often delayed by hours or days, especially gas. 

"""
import os
import sys
import requests
from requests.auth import HTTPBasicAuth 
import datetime
import pandas as pd
import logging
from time import sleep
import json

from loadconfig import getApiKey, getAccountId, getDataDir
from influxconfig import getInfluxUrl, getMeasurementName

PRODUCTS_URL = 'https://api.octopus.energy/v1/products'
ACCTDETS_URL = 'https://api.octopus.energy/v1/accounts'
ELEC_URL = 'https://api.octopus.energy/v1/electricity-meter-points'
GAS_URL = 'https://api.octopus.energy/v1/gas-meter-points'

CAL_VAL = 39.2
VOL_CORR = 1.02264
JOULE_CORR = 3.6

log = logging.getLogger('octopus')


def getOctopusMeters():
    # assumes only one property and one meter per property
    url = f'{ACCTDETS_URL}/{getAccountId()}/'
    try:
        r = requests.get(url, auth=(getApiKey(),''))
        data = r.json()
        mpans = []
        esns = []
        isexps = []
        for mp in data['properties'][0]['electricity_meter_points']:
            mpans.append(mp['mpan'])
            esns.append([x['serial_number'] for x in mp['meters']])
            isexps.append(mp['is_export'])

        mprn = data['properties'][0]['gas_meter_points'][0]['mprn']    
        gsns = [x['serial_number'] for x in data['properties'][0]['gas_meter_points'][0]['meters']]
        return mpans, esns, isexps, mprn, gsns 
    except Exception:
        log.error('failed to get meter ids')
        return ['2000008641754'], [['19L3451361']], [False], '4020791204', ['E6S17033721961']


def getOctopusTariffs():
    url = f'{ACCTDETS_URL}/{getAccountId()}/'
    currdt = datetime.datetime.now()
    try:
        export_tariff = 'None'
        elec_tariff = 'None'
        gas_tariff = 'None'
        print(url)
        r = requests.get(url, auth=(getApiKey(),''))
        if r.status_code != 200:
            print('unable to get data at this time, assuming tariffs')
            return 'E-1R-GO-VAR-26-02-11-H', 'E-1R-VAR-22-11-01-H', 'OUTGOING-VAR-24-10-26-H'
        data = r.json()
        for mps in data['properties'][0]['electricity_meter_points']:
            agrs = mps['agreements']
            if len(agrs) != 0:
                for agr in agrs:
                    fromdt = datetime.datetime.strptime(agr['valid_from'][:19], '%Y-%m-%dT%H:%M:%S')
                    if fromdt <= currdt and agr['valid_to'] is None:
                        if mps['is_export']:
                            export_tariff = agr['tariff_code']
                        else:
                            elec_tariff = agr['tariff_code']

                        break
                    todt = datetime.datetime.strptime(agr['valid_to'][:19], '%Y-%m-%dT%H:%M:%S')
                    if fromdt <= currdt and todt >= currdt:
                        elec_tariff = agr['tariff_code']
                        if mps['is_export']:
                            export_tariff = agr['tariff_code']
                        else:
                            elec_tariff = agr['tariff_code']
                        break
        agrs = data['properties'][0]['gas_meter_points'][0]['agreements']
        for agr in agrs:
            fromdt = datetime.datetime.strptime(agr['valid_from'][:19], '%Y-%m-%dT%H:%M:%S')
            if fromdt <= currdt and agr['valid_to'] is None:
                gas_tariff = agr['tariff_code']
                break
            todt = datetime.datetime.strptime(agr['valid_to'][:19], '%Y-%m-%dT%H:%M:%S')
            if fromdt <= currdt and todt >= currdt:
                gas_tariff = agr['tariff_code']
                break
        return elec_tariff, gas_tariff, export_tariff
    except Exception as e:
        log.error('failed to get meter or tariff data')
        log.error(e)
        return 'E-1R-GO-VAR-26-02-11-H', 'E-1R-VAR-22-11-01-H', 'OUTGOING-VAR-24-10-26-H'


def saveAsCsv(thisdf, typ, outdir):
    # convert data to time-indexed dataframe
    yr = datetime.datetime.now(datetime.timezone.utc).year
    thisdf.set_index('timestamp', inplace=True)
    thisdf.drop(columns=['interval_start','interval_end'], inplace=True)
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
    log.info(f' saved {typ} - latest value is { thisdf.iloc[-1].consumption} at {thisdf.iloc[-1].name}')
    return


def getOneDataset(meterid, serialno, elecdata=True, daysback=7):
    db1 = min(20, daysback)
    db2 = 0
    datadf = None
    while True: 
        p1 = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=-db1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        p2 = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=-db2)).strftime('%Y-%m-%dT%H:%M:%SZ')
        log.info(f'get data for {p1} to {p2} for {meterid} with {serialno}')
        if elecdata:
            urlroot = ELEC_URL
        else:
            urlroot = GAS_URL
        url = f'{urlroot}/{meterid}/meters/{serialno}/consumption/?page_size=1000&period_from={p1}&period_to={p2}&order_by=period'
        tmpdatadf = None
        try:
            r = requests.get(url, auth=(getApiKey(),''))
            data = r.json()
            if 'count' in data:
                if data['count'] > 0:
                    tmpdatadf = pd.DataFrame(data['results'])
                    tmpdatadf['timestamp'] = [datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S%z') for x in tmpdatadf.interval_end]
                    log.info(f'got data for {serialno}')
        except Exception:
            log.error(f'unable to get data from {urlroot}')
        if daysback < 21:
            return tmpdatadf
        else:
            if tmpdatadf is not None:
                if datadf is not None:
                    datadf = pd.concat([datadf, tmpdatadf])
                else:
                    datadf = tmpdatadf
        db2=db1
        db1 = min(db1 + 20, daysback)
        if db2 == db1:
            break
    return datadf


def getPrice(dt, meastype='electricity', daysback=7, amt=None):
    """
    getPrice retrieves the current price for a unit of electricity or gas
    
    :param dt: datetime to request price for
    :param meastype: 'electricity' or 'gas'
    :param daysback: number of days price data to request - default 7
    """
    if amt and amt < 0:
        meastype = 'outgoing'
    if os.path.isfile(f'{meastype}_tariffs.json'):
        fileage = datetime.datetime.now(tz=datetime.timezone.utc).timestamp() - os.stat(f'{meastype}_tariffs.json').st_mtime 
        fileage /= 86400
        if fileage < 7:
            data = json.loads(open(f'{meastype}_tariffs.json', 'r').read())
            if meastype == 'gas':
                prices = [float(d['value_inc_vat']) for d in data['results'] if d['payment_method']=='DIRECT_DEBIT']
                return prices[0]
            dtto = [d['valid_to'] for d in data['results']]
            dtfr = [d['valid_from'] for d in data['results']]
            to_dts = [datetime.datetime(2100,1,1,tzinfo=datetime.timezone.utc) if d is None else datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc) for d in dtto]
            fr_dts = [datetime.datetime(2100,1,1,tzinfo=datetime.timezone.utc) if d is None else datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc) for d in dtfr]
            prices = [float(d['value_inc_vat']) for d in data['results']]
            min_fr = min(fr_dts)
            if dt < min_fr:
                return prices[fr_dts.index(min_fr)]
            for f,t,p in zip(fr_dts,to_dts,prices):
                if f <= dt and t > dt:
                    return p

    # if the file doesn't exist or its too old or if the daterange isn't in it, then call the api
    print('no current tariff data, retrieving latest')
    etariff, gtariff, outgoinget = getOctopusTariffs()

    tariff = gtariff if meastype == 'gas' else etariff
    tariff = outgoinget if meastype == 'outgoing' else etariff

    base_tariff = tariff[5:-2]
    fromdt = (datetime.datetime.now() - datetime.timedelta(days=daysback)).replace(hour=0, minute=0, microsecond=0)
    todt = fromdt + datetime.timedelta(days=daysback+2)
    url = f'{PRODUCTS_URL}/{base_tariff}/{meastype}-tariffs/{tariff}/' \
        f'standard-unit-rates/?period_from={fromdt.strftime("%Y-%m-%dT%H:%M:%SZ")}&period_to={todt.strftime("%Y-%m-%dT%H:%M:%SZ")}'
    try:
        r = requests.get(url)
        data = r.json()
        open(f'{meastype}_tariffs.json', 'w').write(json.dumps(data))
        if meastype == 'gas':
            prices = [float(d['value_inc_vat']) for d in data['results'] if d['payment_method']=='DIRECT_DEBIT']
            return prices[0]
        dtto = [d['valid_to'] for d in data['results']]
        dtfr = [d['valid_from'] for d in data['results']]
        # handle Nulls
        to_dts = [datetime.datetime(2100,1,1,tzinfo=datetime.timezone.utc) if d is None else 
                    datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc) for d in dtto]
        fr_dts = [datetime.datetime(2100,1,1,tzinfo=datetime.timezone.utc) if d is None else 
                    datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc) for d in dtfr]
        prices = [float(d['value_inc_vat']) for d in data['results']]
        min_fr = min(fr_dts)
        if dt < min_fr:
            return prices[fr_dts.index(min_fr)]
        for f,t,p in zip(fr_dts,to_dts,prices):
            if f <= dt and t > dt:
                return p

    except Exception:
        print('unable to connect to API')
    return 0


def getDataFromOctopus(mpans, esns, isexps, mprn, gsns, daysback=7):
    for i,mpan in enumerate(mpans):
        isexp = isexps[i]
        for esn in esns[i]:
            print(mpan, esn)
            datadf = getOneDataset(mpan, esn, True, daysback=daysback)
            if datadf is not None: 
                try:
                    exprtyp = 'electricity' if not isexp else 'export'
                    saveAsCsv(datadf.copy(True), exprtyp, getDataDir())
                    updateInfluxDB(datadf.copy(True), exprtyp, getDataDir())
                except Exception as e:
                    print(e)
    for gsn in gsns:
        print(mprn, gsn)
        datadf = getOneDataset(mprn, gsn, False, daysback=daysback)
        if datadf is not None: 
            try:
                saveAsCsv(datadf.copy(True), 'gas', getDataDir())
                updateInfluxDB(datadf.copy(True), 'gas', getDataDir())
            except Exception as e:
                print(e)
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
    daysback = 7
    if len(sys.argv) > 1:
        daysback = int(sys.argv[1])

    os.makedirs(getDataDir(), exist_ok=True)
    logpath = os.path.expanduser('~/logs')
    os.makedirs(logpath, exist_ok=True)
    logname=os.path.join(logpath, 'octopus.log')    
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(logname, 'a+')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(fh)

    log.info('Starting data capture')
    local_path =os.path.dirname(os.path.abspath(__file__))
    inprogressflag = os.path.join(local_path, 'inprogress')
    while os.path.isfile(inprogressflag):
        log.info('another run in progress, waiting 60s')
        sleep(60)
    open(inprogressflag, 'w').write('1\n')

    try:
        mpans, esns, isexps, mprn, gsns = getOctopusMeters()
        if mpans:
            getDataFromOctopus(mpans, esns, isexps, mprn, gsns, daysback=daysback)

        log.info('Finished')
    except Exception:
        log.info('Failed to get data from octopus')
    os.remove(inprogressflag)
