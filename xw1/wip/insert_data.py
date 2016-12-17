import csv
from timeit import default_timer as timer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime

start_time = timer()
cluster =  Cluster()
session = cluster.connect('xweather')

query="insert into raw_temperature(geohash_id,cal_bucket,event_time,temp,bar) values (?,?,?,?,?)"
prepared = session.prepare(query)

#prep = session.prepare("insert into raw_temperature(geohash_id,cal_bucket,event_time,temp,bar) values (?,?,?,?,?)") 
#batch=BatchStatement()
f = open('c.csv')
rows = csv.reader(f)

for row in rows:
    row[2] = datetime.strptime(row[2],"%Y-%m-%d %H:%M:%S")
    row[3] = float(row[3]) 
    row[4] = float(row[4]) 
    bound = prepared.bind((row[0],row[1],row[2],row[3],row[4]))
    #batch.add(prep,(row[0],row[1],row[2],row[3],row[4]))
   # session.execute("insert into raw_temperature(geohash_id,cal_bucket,event_time,temp,bar) values ('{}','{}','{}',{},{})".format(row[0],row[1],row[2],row[3],row[4]))
    session.execute_async(bound)

#session.execute(batch)

print ("All Done in ", timer() - start_time)
