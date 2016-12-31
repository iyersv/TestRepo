from bottle import route,run,request,response,template
import get_metrics
import kafka_insert_data
import sys
import datetime
import helper_functions

@route('/')
def hello():
    return "Hello World!"

@route('/get_metrics')
def recipes_list():
       conf = helper_functions.get_config()
       place = request.query.place
       if (place == ''):
         lat = request.query.lat
         lon = request.query.long
       else:
          place =place.replace(',',' ')
          lat,lon= helper_functions.get_lat_long(conf,place)
       lat_long = (lat,lon)
       j = get_metrics.validate_inputs(lat_long)
       k = get_metrics.get_neighbors(j[0])
       #  Now get the raw data for the station id.  Stop if there is one
       all_stations = []
       all_data = []
       all_stations = [(nrow['station_id'],nrow['distance']) for nrow in k if nrow['distance'] <30 ]
       #for nrow in k:
	  # all_stations.append((nrow['station_id'],nrow['distance'])	
       for row in k:
	try:
           print row['station_id']
	   v_station_id = row['station_id']
           v_distance = row['distance']
	   j=get_metrics.get_metrics(row['station_id'],datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m'))
	   for k,v in j.iteritems():
	      all_data.append((k,v['metric_value'],v['event_time']))
	   if len(all_data) >0:
        	return template('v',post=all_data,pos=lat_long,station_id=v_station_id,distance=v_distance,all_stations=all_stations)
	except:
	   print('No Data Found',sys.exc_info()[0])
	   return

@route('/forum')
def display_forum():
    forum_id = request.query.id
    page = request.query.page or '1'
    return template('Forum ID: {{id}} (page {{page}})', id=forum_id, page=page)

run(host='vm1', port=8080, debug=True,reloader=True)
