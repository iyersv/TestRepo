from bottle import route,run,request,response,template
import get_metrics
import kafka_insert_data

@route('/')
def hello():
    return "Hello World!"

@route('/get_metrics')
def recipes_list():
       lat = request.query.lat
       long = request.query.long
       lat_long = (lat,long)
       #lat_long =(22.2842, 70.7683)
       j = get_metrics.validate_inputs(lat_long)
       k = get_metrics.get_neighbors(j[0])
       #  Now get the raw data for the station id.  Stop if there is one
       all_data = []
       for row in k:
           print row['station_id']
	   v_station_id = row['station_id']
           v_distance = row['distance']
           j = kafka_insert_data.query_raw_data(row['station_id'],'2016-12-15')
           for data in j:
               #print (data['metric_name'],data['metric_value'])
               all_data.append((data['metric_name'],data['metric_value'],data['event_time']))
	   if len(all_data) >0:
        	return template('v',post=all_data,pos=lat_long,station_id=v_station_id,distance=v_distance)

           # return {'data':all_data}

@route('/forum')
def display_forum():
    forum_id = request.query.id
    page = request.query.page or '1'
    return template('Forum ID: {{id}} (page {{page}})', id=forum_id, page=page)

run(host='vm1', port=8080, debug=True,reloader=True)
