import pandas as pd
import time
import csv
import pygeohash

from cassandra.cluster import Cluster
from kafka import KafkaProducer

import  timeit

def push_to_pandas(filename):
	cluster = Cluster()
        session = cluster.connect('xweather')

	producer = KafkaProducer(bootstrap_servers=['vm1:9092'])

	df = pd.read_csv(filename)
	df1= df[['id','lat','lon','src','elev','timezone','tzoffset']].drop_duplicates()
	df1.src.fillna('NA')
	# Adding Geohash Id
	df1['geohash_id']=df.apply(lambda row:pygeohash.encode(row['lat'],row['lon']),axis=1)

	
	#Now loop through the Dataframe
	for row in df1.itertuples():
	  j = ','.join((row[8],str(row[1]),str(row[5]),row[8][:3],str(row[2]),str(row[3]),str(row[4]),str(row[6]),str(row[7])))
	  future = producer.send('topic-weather-stations',j)
 	  
	print('Completed insert into weather stations')
	
	#Now to the facts
	#Remove the descriptive columns
	df.drop(df.columns[[1,2,3,4,5,6]],axis=1,inplace=True)
	
	#Unpivot the dataset
	df=pd.melt(df,id_vars=['id','timestamp','dateTime'])
	df=df.dropna()
	# Kafka it
	start_time = timeit.default_timer()
	ctr =0;
	producer = KafkaProducer(bootstrap_servers=['vm1:9092'])
	for row in df.itertuples():
	   k=list(row)
	   k=k[1:]
	   j= ','.join(str(x) for x in k)
	   future = producer.send('topic-weather-data',j)
           ctr+=1
        print('Producer timing is ',timeit.default_timer() - start_time,'Rows:',ctr)
	producer.flush()
	producer.close()

#Main function
push_to_pandas('allstations.csv')
#push_to_pandas('india.csv')

