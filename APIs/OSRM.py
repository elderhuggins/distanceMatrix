from urllib.parse import urlencode
from time import sleep
import os, sqlite3
from itertools import product

import requests
import pandas as pd
from local_host import data_dir
driving_db = os.path.join(data_dir, 'driving.db')

def driving(coords, sources=None, destinations=None):
    sleep(0.2) # I have no idea what the real rate limit is
    # Cannot exceed 10,000 distances
    # if `sites` is a list, then assign: coords = ';'.join(f'{s.lon},{s.lat}' for s in sites)
    # if `sites` is a DataFrame, then assign: coords = ';'.join(f'{s.lon},{s.lat}' for _,s in sites.iterrows())
    # returns a json that includes a 'distances' param which is an array of {#_of_sources} arrays of
    # {#_of_destinations} distances each
    params = {'annotations': 'distance' }#, 'exclude': 'toll'}
    # The `exclude` parameter is currently broken due to an extant bug in the OSRM backend
    # https://github.com/Project-OSRM/osrm-backend/issues/6031
    if sources: params['sources'] = ';'.join(map(str,sources))
    if destinations: params['destinations'] = ';'.join(map(str,destinations))
    url = 'http://router.project-osrm.org/table/v1/driving/' + coords
    
    print(f'Downloading distance matrix from OSRM for {url[:60]}' + ('...' if len(url) > 60 else ''))
    payload = urlencode(params, safe=',;')
    response = requests.get(url, params=payload)
    print(f'Response code: {response.status_code}')
    # return json.loads(response.text)
    return {
        'request-url': response.url,
        'response': response
        }

def create_driving_table(connection):
    curse = connection.cursor()
    curse.execute("""
        CREATE TABLE IF NOT EXISTS driving(
            A_lon FLOAT NOT NULL,
            A_lat FLOAT NOT NULL,
            B_lon FLOAT NOT NULL,
            B_lat FLOAT NOT NULL,
            dist_m FLOAT NOT NULL
        );
        """)
    curse.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS directed_edge
        ON driving(A_lon, A_lat, B_lon, B_lat);
        """)
    curse.execute("""
        CREATE INDEX IF NOT EXISTS source
        ON driving(A_lon, A_lat);
        """)
    curse.execute("""
        CREATE INDEX IF NOT EXISTS destination
        ON driving(B_lon, B_lat);
        """)
    connection.commit()

def build_distance_matrix(sources, destinations):
    print('Building distance matrix')
    xlim = 70

    if isinstance(sources, pd.DataFrame): sources = [row for _,row in sources.iterrows()]
    if isinstance(destinations, pd.DataFrame): destinations = [row for _,row in destinations.iterrows()]
    Sset = set((x.lon, x.lat) for x in sources)
    Dset = set((x.lon, x.lat) for x in destinations)

    connection = sqlite3.connect(driving_db)
    create_driving_table(connection)
    curse = connection.cursor()
    def _exit():
        print("Done building distance matrix")
        print()
    def purge_lists(Sset, Dset, verbose=True):
        def purge_Sset():
            if verbose: print('purging source list of length', len(Sset))
            for s in sorted(Sset):
                curse.execute("""
                    SELECT B_lon,B_lat from driving WHERE
                    A_lon=? AND A_lat=?;
                    """, s)
                B_points = set(curse.fetchall())
                if Dset.issubset(B_points): Sset.remove(s)
            if verbose: print('purged source list has length', len(Sset))
        def purge_Dset():
            if verbose: print('purging destination list of length', len(Dset))
            for d in sorted(Dset):
                curse.execute("""
                    SELECT A_lon,A_lat from driving WHERE
                    B_lon=? AND B_lat=?;
                    """, d)
                A_points = set(curse.fetchall())
                if Sset.issubset(A_points): Dset.remove(d)
            if verbose: print('purged destination list has length', len(Dset))
            
        if len(Sset) <= len(Dset): purge1, purge2 = purge_Sset, purge_Dset
        else: purge1, purge2 = purge_Dset, purge_Sset
        
        if len(Sset) == 0 or len(Dset) == 0:
            if verbose: _exit()
            return (Sset, Dset)
        purge1()
        if len(Sset) == 0 or len(Dset) == 0:
            if verbose: _exit()
            return (Sset, Dset)
        purge2()
        if len(Sset) == 0 or len(Dset) == 0:
            if verbose: _exit()
            return (Sset, Dset)
        return (Sset, Dset)
    Sset, Dset = purge_lists(Sset, Dset)
    if len(Sset) == 0 or len(Dset) == 0: connection.close(); return

    Slist, Dlist = sorted(Sset), sorted(Dset)
    total_matrices = -(-len(Slist)//xlim) * -(-len(Dlist)//xlim)
    for i,(s,d) in enumerate(product( range(0,len(Slist),xlim), range(0,len(Dlist),xlim) )):
        print(f"{i+1}/{total_matrices} ", end='')
        ThisBlockSset = set(Slist[s:s+xlim])
        ThisBlockDset = set(Dlist[d:d+xlim])
        ThisBlockSset, ThisBlockDset = purge_lists(ThisBlockSset, ThisBlockDset, verbose=False)
        ThisBlockSlist, ThisBlockDlist = sorted(ThisBlockSset), sorted(ThisBlockDset)
        Sreq = [f'{lon},{lat}' for lon,lat in ThisBlockSlist]
        Dreq = [f'{lon},{lat}' for lon,lat in ThisBlockDlist]
        Slen, Dlen = len(Sreq), len(Dreq)
        result = driving(';'.join(Sreq+Dreq), range(Slen), range(Slen,Slen+Dlen))
        distances = result['response'].json()['distances']

        for I_S, I_D in product(range(Slen), range(Dlen)):
            curse.execute("""
            REPLACE INTO driving(A_lon, A_lat, B_lon, B_lat, dist_m)
            VALUES(?,?,?,?,?);
            """,ThisBlockSlist[I_S]+ThisBlockDlist[I_D]+(distances[I_S][I_D],))

        connection.commit()
    connection.close()
    _exit()    

def driving_distance(A, B, connection):
    curse = connection.cursor()
    curse.execute("""
        SELECT dist_m from driving
        WHERE A_lon=:A_lon AND A_lat=:A_lat AND B_lon=:B_lon AND B_lat=:B_lat;
    """, {"A_lon": A.lon, "A_lat": A.lat, "B_lon": B.lon, "B_lat": B.lat})
    return curse.fetchone()[0]
