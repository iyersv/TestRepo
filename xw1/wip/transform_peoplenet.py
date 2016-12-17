import csv
import pygeohash
import haversine
import mzgeohash
import time
from timeit import default_timer as timer

def read_csv(filename,errors='warn'):
  '''
   reads a csv file with  types associated to each column
  '''
  if errors not in {'warn','ignore','severe','debug','errors'}:
    raise ValueError(" errors must be one of 'warn','ignore','severe','debug','errors'",'Passed was ',errors)

  records=[]   #List of records

  with open(filename,'r') as f:
           rows = csv.reader(f,delimiter=',',quotechar='"')
           #headers = next(rows)
           for i,parts in enumerate(rows,start=1):
               if parts:
                 try:

 		   #parts = [ func(val) for func,val in zip(types,parts) ]
		   parts1=[]
		   parts[0]=parts[0].decode('utf-8-sig')
		   timepart =parts[0].split()
		   parts1.insert(0,pygeohash.encode(float(parts[1]),float(parts[2])))
                   parts1.insert(1,timepart[0])
		   parts1.insert(2,parts[0][:-4])
		   parts1.insert(3,float(parts[3]))
		   parts1.insert(4,float(parts[4]))
                 except:               
                   if errors =='warn':
                      print('Bad Row ',parts, 'Row #',i)
                   continue   #Skips to next row
               records.append(parts1) 
  return records

#start_time=time.time()
start_time=timer()
records=[]
#records=read_csv('2016-04a.csv')
records=read_csv('b.csv')

myfile=open('c.csv','wb')
wr=csv.writer(myfile)
for rows in records:
  wr.writerow(rows)
myfile.close()
#print("Program took ",time.time()-start_time,"to run")
print("Program took ",timer()-start_time,"to run")
