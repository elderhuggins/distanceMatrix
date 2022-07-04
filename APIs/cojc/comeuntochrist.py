#comeuntochrist.py
#this script contains functions to scrape data from comeuntochrist.org

import os
from time import sleep
from types import SimpleNamespace

from local_host import data_dir
from helpers import shelve_it, safe_get

def identify( lon, lat, delay=0.2 ):
    url = "https://ws.churchofjesuschrist.org/ws/maps/v1.0/services/rest/layer/identify.json"
    params = {
        "coordinates": ','.join( (str(lon),str(lat)) ),
        "client": "mapsClient",
    }
    headers = {"Origin": "https://www.churchofjesuschrist.org"}

    sleep(delay) # to prevent server timeout
    return safe_get(url, params=params, headers=headers)

def missionaryPhoneNumbers( unitIds, delay=0.2 ):
    '''
    unitIds has to be a sequence of strings
    '''
    url = "https://www.churchofjesuschrist.org/comeuntochrist/api/missionaryPhoneNumbers"
    params = {"unitIds": ','.join(unitIds)}

    sleep(delay) # to prevent server timeout
    return safe_get(url, params=params)

def get_geocode( address, delay=0.2 ):
    print(f'getting geocode for {address}')
    url = 'https://ws.churchofjesuschrist.org/ws/maps/v1.0/services/rest/geo/geocode.json'
    params = {
        'address': address,
        'client': 'MormonOrg'
    }
    headers = {'Origin': 'https://www.churchofjesuschrist.org'}
    sleep(delay)
    return safe_get(url, params=params, headers=headers)
@shelve_it(os.path.join(data_dir,'geocode_db','geocode_db'), expire=3600*24*7*6*2)
def geocode(address):
    r = get_geocode(address)
    if not r.ok: raise
    coord = r.json()[0]['coordinates']
    geo = SimpleNamespace(**{'lat': coord[1], 'lon': coord[0]})
    return geo