# copyright mark mcintyre 2024-

# tests for octopus api process

import os
from octopus import getOctopusMeters, getOneDataset, saveAsCsv, convertToIdbFmt, updateInfluxDB
from influxconfig import getInfluxUrl, getMeasurementName


def test_getMeters():
    mpan, esns, mprn, gsns = getOctopusMeters()
    assert mpan
    assert mprn
    assert esns
    assert gsns


def test_getElecData():
    mpan, esns, _, _ = getOctopusMeters()
    df = getOneDataset(mpan, esns[1])
    assert df is not None


def test_getGasData():
    _, _, mprn, gsns = getOctopusMeters()
    df = getOneDataset(mprn, gsns[0], False)
    assert df is not None


def test_saveAsCsv():
    mpan, esns, _, _ = getOctopusMeters()
    df = getOneDataset(mpan, esns[1])
    datadir = os.getenv('TEMP')
    saveAsCsv(df,'electricity', datadir)
    assert os.path.isfile(os.path.join(datadir, 'electricity-2024.csv'))


def test_getMeasName():
    meas = getMeasurementName('electricity')
    assert meas == 'HouseElectricityPower'


def test_getMeasNameBad():
    meas = getMeasurementName('fobozz')
    assert meas is None


def test_getInfluxUrl():
    url, user, pwd = getInfluxUrl()
    assert user
    assert pwd
    assert url


def test_convertToIdbElec():
    mpan, esns, _, _ = getOctopusMeters()
    df = getOneDataset(mpan, esns[1])
    meas = getMeasurementName('electricity')
    outdir = os.getenv('TEMP')
    idbd = convertToIdbFmt(df, meas, outdir)
    assert idbd


def test_convertToIdbGas():
    _, _, mprn, gsns = getOctopusMeters()
    df = getOneDataset(mprn, gsns[0], False)
    meas = getMeasurementName('gas')
    outdir = os.getenv('TEMP')
    idbd = convertToIdbFmt(df, meas, outdir)
    assert idbd


def test_updateInfluxDB():
    _, _, mprn, gsns = getOctopusMeters()
    df = getOneDataset(mprn, gsns[0], False)
    outdir = os.getenv('TEMP')
    res = updateInfluxDB(df, 'gas', outdir)
    assert res > 0
