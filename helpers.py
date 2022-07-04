import os, json, pickle
from time import time,sleep
from itertools import count

import yaml, requests

data_dir = '_data'
property_db = os.path.join(data_dir, 'property-info.db')
households_dir = os.path.join(data_dir, 'households')

d_one_transfer = 10**10#60*60*24*(7*6-1)

sector_colors = ['#f4cccc', '#fce5cd', '#fff2cc', '#d9ead3', '#c9daf8', '#d9d2e9']
ceildiv = lambda a,b: -(-a // b)

def safe_get(url, accepted_codes=[], max_tries=None, **kw):
    for n in count(1):
        if max_tries and max_tries<n: return None
        try:
            print(f'downloading {url}')
            response = requests.get(url, **kw)
            print(f"Response code: {response.status_code}")
            if response.status_code in [200]+accepted_codes:
                return response
        except requests.exceptions.ConnectionError:
            pass
        s = int(n**.5+1)
        print(f'download failed - sleeping for {s} seconds - {url}')
        sleep(s)

def memoize_if_current(f):
    memo = dict()
    def g(*args):
        if (args not in memo) or (g.current) != True:
            memo[args] = f(*args)
            g.current = True
        return memo[args]
    return g

def disk_memoize(**kw):
    if kw.get('mode', None) == 'json':
        ext, load, dump, binary = '.json', json.load, json.dump, ''
    elif kw.get('mode', None) == 'pickle':
        ext, load, dump, binary = '.pkl', pickle.load, pickle.dump, 'b'
    else:
        ext, load, dump, binary = '', lambda f: f.read(), (lambda data, f: f.write(data)), ''
    def too_old(fp):
        if 'maxage' not in kw: return False
        return time() - os.path.getmtime(fp) > kw['maxage']
    def decorator(func):
        def new_func(*args, **kwargs):
            dn = os.path.join(kw.get('dn', ''), kwargs.get('sd', ''))
            if dn != '': os.makedirs(dn, exist_ok=True)
            fn = f"{kwargs.get('name', kwargs.get('fn', kw.get('name', kw.get('fn'))))}{ext}"
            fn = kwargs.get('fn', fn) # user can overwrite the filename if necessary
            fp = os.path.join(dn, fn)
            fp = kwargs.get('fp', fp) # user can overwrite the filepath if necessary
            if os.path.exists(fp) and not too_old(fp):
                if kwargs.get("just_check", False) == True: return True
                with open(fp, f'r{binary}') as f:
                    return_value = load(f)
            else:
                if kwargs.get("just_check", False) == True: return False
                return_value = func(*args, **kwargs)
                if return_value == "DELETE_ME":
                    if os.path.exists(fp): os.remove(fp)
                    return None
                if (kw.get('mode', None) == 'json') and (type(return_value) in [str, bytes, bytearray]):
                    return_value = json.loads(return_value)
                with open(fp, f'w{binary}') as f:
                    if 'indent' in kw: dump(return_value, f, indent=kw['indent'])
                    else: dump(return_value, f)
            return return_value
        return new_func
    return decorator

def shelve_it(fp, expire=None):
    import shelve
    
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    def decorator(func):
        db = shelve.open(fp)
        
        def new_func(*args):
            dn = os.path.dirname(fp)
            if dn != '': os.makedirs(dn, exist_ok=True)

            now, key = time(), yaml.dump(args)
            if key not in db or (expire and now-db[key]['TS'] > expire):
                db[key] = {'TS': now, 'data': func(*args)}
                db.sync()
            return db[key]['data']

        return new_func
    return decorator

def yaml_memoize(func):
    d = dict()
    def new_func(*args):
        key = yaml.dump(args) # serialized arguments
        return d[key] if key in d else d.setdefault(key, func(*args))
    return new_func