# config file
import os


def getDataDir():
    datadir = 'DATADIRSUBST'
    return os.path.expanduser(datadir)


def getApiKey():
    passwordfile = os.path.expanduser('~/.ssh/octopus_apikey')
    with open(passwordfile) as inf:
        passwd = inf.readline().strip()
    return passwd


def getAccountId():
    return 'A-44EFFE0D'
