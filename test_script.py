import os
import sqlite3

import local_host
from APIs.cojc.comeuntochrist import geocode
from APIs.OSRM import build_distance_matrix, driving_distance, driving_db

def main():
    this_script_directory = os.path.dirname( os.path.realpath(__file__) )
    filepath = os.path.join(this_script_directory, 'address_test_data.txt')
    with open(filepath) as f:
        addresses = [line.strip() for line in f.readlines()]
    geos = [geocode(a) for a in addresses]
    build_distance_matrix(geos, geos)

    connection = sqlite3.connect(driving_db)    
    from itertools import product
    for a,b in product(addresses, addresses):
        print()
        print('the shortest driving distance from')
        print(a)
        print('to')
        print(b)
        print(f'is {driving_distance(geocode(a), geocode(b), connection)} meters')
        print()
    connection.close()

if __name__ == '__main__':
    main()