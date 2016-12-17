from timeit import default_timer as timer
from cassandra.cluster import Cluster
from datetime import datetime

start_time = timer()
cluster =  Cluster()
session = cluster.connect('xweather')

def insert_vals(data):
#Assuming that the data is in the format (geohash_id,cal_bucket,event_time,temp,ar)
	ctr =0;
	query="insert into raw_temperature(geohash_id,cal_bucket,event_time,temp,bar) values (?,?,?,?,?)"
	prepared = session.prepare(query)


	for row in data:
	 row = list(row.split(','))
   	 row[2] = datetime.strptime(row[2],"%Y-%m-%d %H:%M:%S")
   	 row[3] = float(row[3]) 
    	 row[4] = float(row[4]) 
   	 bound = prepared.bind((row[0],row[1],row[2],row[3],row[4]))
   	 session.execute_async(bound)
	 ctr+=1
	 #print('Executed row',row)
	print('Executed dataset in ',timer()-start_time,'rows:',ctr)
