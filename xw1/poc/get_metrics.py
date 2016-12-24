from timeit import default_timer as timer
import pygeohash
import mzgeohash
import haversine
import kafka_insert_data
import pandas as pd
import datetime
import sys
from operator import itemgetter
import json

def validate_inputs(latlong, reqtype='obs', days=0, metrics='all', maxdistance=30):
    # Validate the inputs passed in.  The validation is primarily that the inputs are present
    # or the defaults get passed out
    return_list = []
    validrequests = ['obs', 'forecast']

    try:
        # Validate the Lat/Long passed
        l_context = 'Get geohash'
        try:
            geohashid = pygeohash.encode(float(latlong[0]), float(latlong[1]))
        except:
            print("Invalid lat long passed.")
            sys.exit(1)

        l_context = 'Get request type'

        # Validate the request type
        request = reqtype
        if request not in validrequests:
            raise ValueError('Invalid Request type :' + str(request), 'valid requests are ', validrequests)

            # Validate the days
        l_context = 'Get days'

        if request != 'obs':
            if (days > 10) or (days <= 0):
                days = 1
        else:
            days = 0

        # Validate Max Distance
        l_context = 'Get maxdistance'

        if maxdistance > 30:
            distance = 30
        else:
            distance = maxdistance

    except ValueError as err:
        print (err.args)
        return -1
    except Exception, err:
        print('Error in inputs while processing ' + l_context, err)
        return -1

    return_list.extend((geohashid, request, days, metrics, distance))
    print('Return list is ', return_list)
    return return_list


def get_neighbors(geohashid):
    # Get the nearest neighbors
    # print('Passed in geohash value is ',geohashid)
    try:
        maplist = list(mzgeohash.neighbors(geohashid[:3]).values())
    except Exception, err:
        print('Error getting the nearest neighors', err)
        maplist = []

    l = []
    try:
        request_coord = pygeohash.decode_exactly(geohashid)
        request_coord = tuple((request_coord[0], request_coord[1]))
    except Exception, err:
        print('Error decoding requesting geohash id into lat long:', err)

    if maplist:
        for n in maplist:
            # print('Maplist', n)
            mdict = kafka_insert_data.query_neighbors(n)
            for row in mdict:
                row['lat_long'] = tuple((float(row['lat_long'][0]), float(row['lat_long'][1])))
                row['station_id'] = row['station_id'].replace("'", "")
                row['distance'] = haversine.haversine(row['lat_long'], request_coord)
                l.append(row)
    l = sorted(l, key=itemgetter('distance'))
    return l
    '''
    try:
        df = pd.DataFrame(l)
        if df.count > 0:
            df = df.sort_values(by='distance')
        return df
    except:
        print('Error building Dataframe as there were no rows')
    '''


# return <array>
def get_metrics(station_id, cal_bucket=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m')):
    l = []
    mdict = kafka_insert_data.query_raw_data(station_id, cal_bucket)
    for row in mdict:
        l.append(row)
    try:
        df = pd.DataFrame(l)
        df['Rank'] = df.groupby(['metric_name'])['event_time'].rank(ascending=False)
        df = df[df['Rank'] == 1.0]
	df.set_index('metric_name',drop=True,inplace=True)
	df=df.drop(['Rank','station_id'],axis=1)
	sdict=df.to_json(orient="index",date_format='iso')
	sdict=json.loads(sdict)
	return sdict        
    except:
        print('Error building Dataframe as there were no rows')


if __name__ == '__main__':
    if __name__ == "__main__":

        # Main Program

        if len(sys.argv) > 1:
            lat_long = tuple(sys.argv[1:3])
        else:
            lat_long = (22.2842, 70.7683)
            print ('As Lat Long was not passed in defaulting ')
        print('Lat Long is ', lat_long)
        j = validate_inputs(lat_long)
        k = get_neighbors(j[0])
        print k
        #  Now get the raw data for the station id.  Stop if there is one
        all_data = []
        for row in k:
            print row['station_id']
            j = kafka_insert_data.query_raw_data(row['station_id'],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m') )
            for data in j:
                print (data['metric_name'], data['metric_value'])
                all_data.append(data)
            if len(all_data) > 0:
                exit()
