# copyright Mark McIntyre, 2024-

# push some energy values to Openhab

import os
from openhab import OpenHAB


def getOpenhabURL():
    passwordfile = os.path.expanduser('~/.ssh/ohpass')
    with open(passwordfile) as inf:
        username = inf.readline().strip()
        passwd = inf.readline().strip()

    ohurl = 'https://{}:{}@myopenhab.org/rest'.format(username, passwd)
    return ohurl


def updateMeterReading(reading, readingdate, elec=True):
    openhab = OpenHAB(getOpenhabURL())
    try:
        OHTimestamp = openhab.get_item('HouseMeterTimestamp')
        if elec is True:
            OHreading = openhab.get_item('HouseElectricityMeterReading')
        else:
            OHreading = openhab.get_item('HouseGasMeterReading')
        OHreading.state = reading
        OHTimestamp.state = readingdate
    except Exception as e:
        print(e)
    return
