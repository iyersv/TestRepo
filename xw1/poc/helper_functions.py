
def get_config():
    from ConfigParser import ConfigParser
    
    conf=ConfigParser()
    conf.read('config/weather.cfg')
    return conf

def get_lat_long(config,address):
    ''' Returns the lat Long from google geocode api based on address.'''

    from urllib import quote_plus
    import logging
    import requests

    qs_dict = {'address':quote_plus(address),'key':config.get('google','api_key'),}
    logging.debug('Requesting weather data from google for %s',address)
    resp = requests.get('https://maps.googleapis.com/maps/api/geocode/json',params=qs_dict)
    
    try:
	lat,lon=resp.json().get('results')[0].get('geometry').get('location').values()
    except KeyError:
	raise Exception('Could not find address %s',address)
    return lat,lon

''' Start Main Function '''

#conf = get_config()
#lat,lon= get_lat_long(conf,'15701 Fremont Way, Apple Valley 55124')
#print ('Value of lat,lon ',lat,lon)
	
