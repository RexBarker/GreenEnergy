#######################################################################
# Loads weather data from JSON files and reports them to 
# kafka topics for curent and forecast weather 
#######################################################################

import os
import sys
import json
import argparse
import logging
from glob import glob, iglob
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from datetime import datetime

#----------------------------------------------------------------------

def pi_log(msg):
    log.info(msg)
    print(msg)

def pw_log(msg):
    log.warn(msg)
    print(msg)

def pe_log(msg):
    log.error(msg)
    print(msg)
    sys.exit()

# kafka client callbacks
def on_send_success(record_metadata):
    log.info(record_metadata.topic)
    log.info(record_metadata.partition)
    log.info(record_metadata.offset)

def on_send_error(excp):
    log.error('failed send:', exc_info=excp)
    # handle exception

# general purpose
def avg(x):
    if x is not None:
        if len(x):
            return sum(x)/len(x)  
        else: 
            return None
    else:
        return None

def getTopicsFromFileglob(fileglob):
    cities = set() 
    for fn in iglob(fileglob):
        try:
            with open(fn,mode='r',encoding='ISO-8859-1') as fp:
                instr = fp.read().replace('\x00','0')
                fdata = json.loads(instr)

            cities.update(fdata.keys())
        except:
            pw_log(f"Warning: failed to load file {fn}, ...skipping")

    return cities

def mapCurrWeatherFlat(cityDat):
    return {    "dt":         cityDat.get('dt')                   or 'NA',                                     # time when information was produced
                "temp":       cityDat.get('main').get('temp')     or 'NA' if cityDat.get('main') else 'NA',    # current temp (Kelvin)
                "temp_min":   cityDat.get('main').get('temp_min') or 'NA' if cityDat.get('main') else 'NA',    # daily low temp (Kelvin)
                "temp_max":   cityDat.get('main').get('temp_max') or 'NA' if cityDat.get('main') else 'NA',    # daily max temp (Kelvin)
                "humidity":   cityDat.get('main').get('humidity') or 'NA' if cityDat.get('main') else 'NA',    # humidity (0-100%)
                "pressure":   cityDat.get('main').get('pressure') or 'NA' if cityDat.get('main') else 'NA',    # pressure (mbar)
                "clouds":     cityDat.get('clouds').get('all')    or 0 if cityDat.get('clouds') else 'NA' ,    # clouds 0 - 100 ( 0 = clear, 100 = no sun)
                "visibility": cityDat.get('visibility')           or 0,                                        # visibility (meters)
                "wind_speed": cityDat.get('wind').get('speed')    or 0  if cityDat.get('wind') else 'NA',      # wind speed (km/h)
                "wind_direc": cityDat.get('wind').get('deg')      or 0  if cityDat.get('wind') else 'NA',      # wind direction (angle in degrees)
                "sunrise":    cityDat.get('sys').get('sunrise')   or 'NA' if cityDat.get('sys') else 'NA',     # sunrise in Epoch time (use python datetime.fromtimestamp() to decode)
                "sunset":     cityDat.get('sys').get('sunset')    or 'NA'   if cityDat.get('sys')  else  'NA'  # sunset (note: both sunrise and sunset are local time including daylight offset)
            }

def mapForeWeatherFlat(cityDat):
    if cityDat.get('list') is None: return None
    fcast = cityDat['list']
    dt_diff = datetime.fromtimestamp(fcast[1]['dt']) - datetime.fromtimestamp(fcast[0]['dt'])
    dt_0 = datetime.fromtimestamp(fcast[0]['dt']) - dt_diff
    outdat = { "dt": int(datetime.timestamp(dt_0)) }   # JSON serializer doesn't allow datetime objects
    outlist = []
    for fc in fcast:
        dat = {
                    "dt": fc.get('dt') or 'NA',
                    "dt_pred": int(datetime.timestamp(dt_0)),
                    "dt_diff": int(datetime.timestamp(dt_0)) - int(datetime.timestamp(datetime.fromtimestamp(fc['dt']))),
                    "temp":       fc.get('main').get('temp')     or 'NA' if fc.get('main') else 'NA',   # current temp (Kelvin)
                    "temp_min":   fc.get('main').get('temp_min') or 'NA' if fc.get('main') else 'NA',   # daily low temp (Kelvin)
                    "temp_max":   fc.get('main').get('temp_max') or 'NA' if fc.get('main') else 'NA',   # daily max temp (Kelvin)
                    "humidity":   fc.get('main').get('humidity') or 'NA' if fc.get('main') else 'NA',   # humidity (0-100%)
                    "pressure":   fc.get('main').get('pressure') or 'NA' if fc.get('main') else 'NA',   # pressure (mbar)
                    "clouds":     fc.get('clouds').get('all')    or 0 if fc.get('clouds') else 'NA' ,   # clouds 0 - 100 ( 0 = clear, 100 = no sun)
                    "wind_speed": fc.get('wind').get('speed')    or 0  if fc.get('wind') else 'NA',     # wind speed (km/h, direction ignored)
                    "wind_direc": fc.get('wind').get('speed')    or 0  if fc.get('wind') else 'NA',     # wind speed (km/h, direction ignored)
              }
        outlist.append(dat)
    
    outdat['list'] = outlist
    return(outdat)


#----------------------------------------------------------------------

logging.basicConfig(filename="./weather_producer.log",filemode="a",
                    format='%(asctime)s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger(__name__)

log.info("--- kafkaProducerWeather Start ---")

parser = argparse.ArgumentParser()
parser.add_argument('-configfile',type=str,dest='configfile',default='config.json',
                    help='Optional: configuration file for kafka producer (default: ./config.json)')

parser.add_argument('-config',type=str,dest='config',default='kafkaWeatherProducer',
                    help='Optional: config section for kafka producer (default: kafkaWeatherProducer)')

parser.add_argument('-connection',type=str,dest='connection',default='local',
                    help='Optional: connection to use for kafka producer [local, internal, external] (default: local))')

parser.add_argument('-cleantopics',dest='cleantopics',action='store_true',
                    help='Optional: removes kafka topics staring with \'w-\', \'wc-\', or \'wf-\'')


args = parser.parse_args()

# check for the configuration file
if not os.path.exists(args.configfile):  
    pe_log(f"config file={args.configfile} did not exist!")

with open(args.configfile,'r') as fp: 
    gen_config = json.load(fp) 
    pi_log(f"loaded config file={args.configfile}")

# the sub configuration for the kafakaProducer
kwp_config = gen_config.get(args.config) or pe_log(f"Could not load specified configuration section as \'{args.config}\'")

# check the configuration file
baseDir = kwp_config.get('baseDir') or pe_log(f"No \'baseDir\' specified in config \'{args.config}\'")
os.path.exists(baseDir) or pe_log(f"Error, specified baseDir={baseDir} did not exist")

# check for the minimum configurations, for measurements
kwp_config.get('measurements') or pe_log(f"Error, \'measurements\' not specified in config section \'{args.config}'")

currWeatherGlobs = kwp_config['measurements'].get('currentWeather')  or \
    pe_log(f"Error, \'currentWeather\' not specified in config section \'{args.config}.measurements'")

foreWeatherGlobs = kwp_config['measurements'].get('forecastWeather') or \
    pe_log(f"Error, \'forecastWeather\' not specified in config section \'{args.config}.measurements'")

# check for minimum server connections
kwp_config.get('bootstrapServers') or \
    pe_log(f"Error: no \'bootstrapServers\' definition found in \'{args.config}\'")

bsServers = kwp_config['bootstrapServers'].get(args.connection) or \
    pe_log(f"Error: no \'{args.connection}\' definition found in \'{args.config}.connection\'")

# read the files to determine which topics to use
# create topics, if they don't exist
cities = set()
for fglob in (*kwp_config['measurements']['currentWeather'], *kwp_config['measurements']['forecastWeather']):
    fglob = os.path.join(baseDir,fglob)
    cities.update(getTopicsFromFileglob(fglob))

# check topics that exist
adm = KafkaAdminClient(bootstrap_servers=bsServers)
consumer = KafkaConsumer(bootstrap_servers=bsServers,group_id='weather')
existingTopics = consumer.topics()

# clean out old topics?
if args.cleantopics:
    cleanTopics = [ t for t in existingTopics if t.startswith('w-') or t.startswith('wc-') or t.startswith('wf-')]
    if cleanTopics: 
        print('\nThe following topics will be removed:\n',','.join(cleanTopics),'\n')
        res = input('enter \'yes\' to proceed:')
        if res == 'yes':
            log.info("removing topics: " + ','.join(cleanTopics))
            try:
                adm.delete_topics(cleanTopics, timeout_ms=10)
            finally:
                pw_log("An error occured while deleting topics")
        else:
            pi_log("Aborted removing of topics")
    else:
        pi_log('No topics found to delete')

newTopics = [ NewTopic(f"wc-{c}",1,2) for c in cities if not f"wc-{c}" in existingTopics]
newTopics.extend([ NewTopic(f"wf-{c}",1,2) for c in cities if not f"wf-{c}" in existingTopics])
if newTopics:
    adm.create_topics(new_topics=newTopics,validate_only=False)
    pi_log('created new topics: ' + str(consumer.topics()))

#########################
# consume files
#########################

# publish to kafka with JSON serialization
producer = KafkaProducer(bootstrap_servers=bsServers,
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))

if kwp_config['dataSource'] == 'files':
    currFiles = sorted(*[ glob(os.path.join(baseDir,f)) for f in currWeatherGlobs])
    foreFiles = sorted(*[ glob(os.path.join(baseDir,f)) for f in foreWeatherGlobs])

    # map the current weather
    for f in currFiles:
        with open(f,'r') as fp:
            currDat = json.load(fp)

        for city,dat in currDat.items():
            topic = f"wc-{city}"
            outdat = mapCurrWeatherFlat(dat)
            if outdat['dt'] != 'NA': 
                producer.send(topic,outdat)
                print(f"current weather city={city}", "time=",datetime.fromtimestamp(outdat['dt']))

    # map the predicted weather
    # weather is produced with +3 hour increments.  Therefore, assume the prediciton time was
    # measurement[0] -3
    for f in foreFiles:
        with open(f,'r') as fp:
            foreDat = json.load(fp)

        for city,dat in foreDat.items():
            topic = f"wf-{city}"
            outdat = mapForeWeatherFlat(dat)
            if outdat is not None: 
                if outdat['dt'] != 'NA': 
                    producer.send(topic,outdat)
                    print(f"forecast weather city={city}", "time=",datetime.fromtimestamp(outdat['dt']))
                else:
                    print(f"no dat reported for city={city}...skipping")
            else:
                print(f"Could not report for city={city}...skipping")

else:
    # implemenation for api currently missing
    pw_log('processing via api source is currently not implemented')
    pass

adm.close()
consumer.close()

producer.flush()

pi_log("kafkaProducerMetrics:")
pi_log("performance:" + str(json.dumps(producer.metrics(),indent=4)))
pi_log("successful completion")
producer.close()