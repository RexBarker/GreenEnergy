#######################################################################
# Consumes weather data from kafka topics and transports them to 
# InfluxDB. Kafka topics were produced using the producer side script
# in the format:
#  "wc-City":  current weather for city (as measured)
#  "wf-City":  forecast weather for city 
#               (from forecast time + 5 days, every 3 hours)
#######################################################################

import os
import sys
import json
import argparse
import logging
import re
from glob import glob, iglob
from confluent_kafka import Consumer, TopicPartition, KafkaError
from influxdb import InfluxDBClient
from datetime import datetime, timezone
import holidays

#----------------------------------------------------------------------

# default logging handlers
def pi_log(msg):
    log.info(msg)
    print(msg)
    return 1

def pw_log(msg):
    log.warn(msg)
    print(msg)
    return 1

def pe_log(msg):
    log.error(msg)
    print(msg)
    sys.exit()

#----------------------------------------------------------------------
# kafka client callbacks
def on_send_success(record_metadata):
    log.info(record_metadata.topic)
    log.info(record_metadata.partition)
    log.info(record_metadata.offset)

def on_send_error(excp):
    log.error('failed send:', exc_info=excp)
    # handle exception

#----------------------------------------------------------------------
# map weather topics into influxDB formatted measurements
def flattenWeather(msg,keepMeasurements=None):
    topic = msg.topic()
    content = json.loads(msg.value().decode('utf-8'))
    
    outLoad = []
    if content.get('list'):
        # was a forecast weather topic
        for l in content['list']:
            dt_diff = str(int(l['dt_diff'] / 3600))
            dt = l['dt_pred']
            measurements = keepMeasurements if keepMeasurements else l.keys()
            outLoad.append( (topic + '_' + dt_diff, dt, [(k,l[k]) for k in measurements if k in l.keys() ] ))
    else:
        # was a current weather topic
        l = content
        dt = l['dt']
        measurements = keepMeasurements if keepMeasurements else l.keys()
        outLoad.append( (topic, dt, [(k,l[k]) for k in measurements if k in l.keys() ] ))
            
    return outLoad

#----------------------------------------------------------------------
# holiday checker, weighted by the relative exports to that country for that month
country_pop ={'AT':  8.352,  # country population in 2010
              'BE': 10.840, 
              'CH':  7.786, 
              'CZ': 10.460,
              'DE': 81.750,
              'DK':  5.535,
              'FR': 64.610,
              'LU':  0.502,
              'NL': 16.570,
              'PL': 38.53,
              'SE':  9.341 }
holiday_op = [ ('AT', holidays.Austria()),   # holidays for each country
               ('BE', holidays.Belgium()),   # implented this way because of bug in library
               ('CH', holidays.Switzerland()),
               ('CZ', holidays.Czech()),
               ('DE', holidays.Germany()),
               ('DK', holidays.Denmark()),
               ('FR', holidays.France()),
               ('LU', holidays.Luxembourg()),
               ('NL', holidays.Netherlands()),
               ('PL', holidays.Poland()),
               ('SE', holidays.Sweden())
              ]

def timedata(dt):
    # create time data extra information for influxDB (makes sorting easier)
    timestamp = datetime.fromtimestamp(dt)
    year = timestamp.year
    month = timestamp.month
    hour = timestamp.hour
    minute = timestamp.minute
    weekday = timestamp.weekday()
    
    # determine the holiday export weight
    # for DE only holiday, holiday = 1.0
    # for DE+ others holidays, holiday > 1.0
    # for only other holidays, holiday < 1.'
    holiday = 1 if holidays.Germany().get(timestamp) else 0    # DE holiday = 1
    for cn,op in holiday_op:
        if cn == 'DE': continue
        if op.get(timestamp):
            holiday += exportWeights[cn][year].get(month-1) \
                if exportWeights and exportWeights.get(cn) and exportWeights[cn].get(year) \
                else country_pop[cn]/sum([pop for country,pop in country_pop.items() if country != 'DE'])

    return { 
                'timestamp': timestamp,
                'year'     : year,
                'month'    : month,
                'hour'     : hour,
                'minute'   : minute,
                'weekday'  : weekday,
                'holiday'  : holiday
           }

#----------------------------------------------------------------------

###########################
# Setup 
###########################

logging.basicConfig(filename="./weather_consumer.log",filemode="a",
                    format='%(asctime)s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger(__name__)

log.info("--- kafkaConsumerWeather Start ---")

parser = argparse.ArgumentParser()
parser.add_argument('-configfile',type=str,dest='configfile',default='config.json',
                    help='Optional: configuration file for kafka producer (default: ./config.json)')

parser.add_argument('-config',type=str,dest='config',default='kafkaWeatherConsumer',
                    help='Optional: config section for kafka consumer (default: kafkaWeatherConsumer)')

parser.add_argument('-connection',type=str,dest='connection',default='local',
                    help='Optional: connection to use for kafka producer [local, internal, external] (default: local))')

parser.add_argument('-removedb',dest='removedb',action='store_true',
                    help='Optional: drops previous database if exists')

args = parser.parse_args()

# check for the configuration file
if not os.path.exists(args.configfile):  
    pe_log(f"config file={args.configfile} did not exist!")

with open(args.configfile,'r') as fp: 
    gen_config = json.load(fp) 
    pi_log(f"loaded config file={args.configfile}")

# the sub configuration for the kafakaConsumer
kwc_config = gen_config.get(args.config) or pe_log(f"Could not load specified configuration section as \'{args.config}\'")

###########################
# Kafka consumer creation
###########################

# check valididity of configuration
kwc_config.get('kafkaConsumer') or \
    pe_log(f"Error: \'kafkaConumer\' specification not found in \'{args.configfile}.{args.config}\'")

kwc_config['kafkaConsumer'].get('bootstrapServers') or \
    pe_log(f"Error: \'bootstapServers\' specification not found in \'{args.configfile}.{args.config}.kafkaConsumer\'")

bsServers = kwc_config['kafkaConsumer']['bootstrapServers'].get(args.connection) or \
    pe_log(f"Error: \'{args.connection}\' specification not found in \'{args.configfile}.{args.config}.kafkaConsumer.bootstrapServers\'")

# check for groups and topics definition
groupsTopics = kwc_config['kafkaConsumer'].get('groups and topics') or \
    pe_log(f"Error: \'groups and topics\' specification not found in \'{args.configfile}.{args.config}.kafkaConsumer\'")

# create connection to Kafka for each group
consumers = []
for group,(topicslist,partition,offset) in groupsTopics.items():

    topicFilter = [ re.compile(pat) for pat in topicslist] 

    con = Consumer({
               'bootstrap.servers': ",".join(bsServers),
               'group.id': group,
               'default.topic.config': { 'auto.offset.reset': 'earliest' }
            })
    thesetopics = [ tpmat.group(0) for tpmat in [ pat.match(topic) for pat in topicFilter for topic in con.list_topics().topics ] if tpmat ]
    if thesetopics: 
        con.assign([ TopicPartition(tp,partition,offset) for tp in thesetopics ])
        
        didAssign = { tpp.topic for tpp in con.assignment() }
        diffAssign = set(thesetopics).difference(didAssign)
        if diffAssign:  
            pe_log(f"Error, something awry: attempt assign topics to consumer group \'{group}\' did not assign topics: {diffAssign}")

        consumers.append((group,con))
        pi_log(f"Created consumer group \'{group}\' with topics {sorted(didAssign)}")

    else:
        pw_log(f"failed to render topics from topics list: \'{topicslist}\'")

# were there any consumer groups created? If not, no point in continuing
if not consumers: pe_log("Failed to create any consumer groups from specified topics...terminating")

###########################
#  InfluxDB creation
###########################
inf_config = gen_config[args.config].get('influxDB') or \
    pe_log(f"Error: \'influxDB\' specification not found in \'{args.configfile}.{args.config}\'")

# check connection to InfluxDB
# default is internal connection
conDB = { 'host': inf_config['connection'][args.connection]['host'],
          'port': inf_config['connection'][args.connection]['port']}

# add extra setup if specified
if inf_config['connection'].get('setup'):
    conDB.update(inf_config['connection']['setup'])
else:
    conDB.update({ 'timeout': 5, 'retries': 3})

# add extra write options, if specified
writeOpts = inf_config.get('write options') or {}

# instatiate Influx client (does not necessarily tell of sucessful connection to database)
try:
    client = InfluxDBClient(**conDB) 
except:
    pe_log(f"Could not connect to InfluxDBClient with following arguments:{conDB}")

# Attempt to create databases as the names of the weather groups
# determine if there are existing databases
dbnames = list(groupsTopics.keys()) 
try:
    dbs = {d['name'] for d in client.get_list_database()}
except:
    pe_log(f"Could not connect to InfluxDBClient with following arguments:{conDB}")

pi_log(f"Successufl connection to InfluxDBClient with following arguments:{conDB}")

# remove old DB, if requested
dbrs = dbs.intersection(set(dbnames))   

if not dbrs and args.removedb: 
    pi_log("No existing databases to drop")  # no database found which match ours
elif args.removedb:
    # else there was, so do something
    for dbr in dbrs:
        print(f'\nThe following database will be removed:\n{dbr}\n')
        res = input('enter \'yes\' to proceed: ')
        if res == 'yes':
            pi_log(f"removing database:{dbr}" )
            try:
                client.drop_database(dbr)
                dbs = {d['name'] for d in client.get_list_database()}
            except:
                pe_log("An error occured while dropping database")
        else:
            pi_log("Aborted removing of databases")

for db in dbnames:
    if not dbs.intersection({db}): 
        client.create_database(db) 
        pi_log(f"Created new influx database \'{db}\'")

###########################
#  Collect and transfer 
###########################
keepMeasurements = kwc_config['dataTransform'].get('keepMeasurements') if kwc_config.get('dataTransform') else None
timeout = kwc_config['kafkaConsumer'].get('timeout',10)   # required timeout (set to -1 in config file for infinite loop, default 10 seconds)
batchSize = kwc_config['kafkaConsumer'].get('batchSize',100) # number of samples per message poll (kafka) and influxDB commit

# weights for export power, for tracking in the database (for holiday exports)
# when read from the json file:
#       exportWeights[dict(country)][dict(year)][list(month0)]
exportWeights = {}
exportWeightsFile = kwc_config['influxDB'].get('export power weight file')
if exportWeightsFile and os.path.exists(exportWeightsFile):
    with open(exportWeightsFile,'r') as f:
        exportWeights = json.load(f)
    pi_log(f"Export weights specified from {exportWeightsFile}")
else:
    pw_log("No \'export power weight file\' config definition found, or file was missing...assuming equal weights")

consumedCount = 0
commitedCount = 0
consumersAlive = len(consumers)

while consumersAlive:
    for (group,con) in consumers:
        # group=database for InfluxDB
        # extract kafka stream batch
        msgs = con.consume(num_messages=batchSize,timeout=timeout)
        if not msgs:
            consumersAlive -= 1
            pi_log(f"Terminated kafkaConsumption for group \'{group}\', consumed {consumedCount} messages")
            break

        # create payload for influxDB
        jsonLoad = []
        usedTopics = set() 
        for msg in msgs: 
            weatherDat = flattenWeather(msg,keepMeasurements)
            for topic,dt,measDat in weatherDat:
                timeDat = timedata(dt)
                usedTopics.update([topic])
                
                # JSON payload formation
                jsonLoad.append(
                                {
                                    "measurement": topic,
                                    "time": timeDat['timestamp'],
                                    "fields": { 
                                                **{ "year": int(timeDat['year']), "month": int(timeDat['month']), 
                                                    "weekday": int(timeDat['weekday']), "holiday": float(timeDat['holiday']),
                                                    "hour": int(timeDat['hour']), "minute": int(timeDat['minute']) },
                                                **{ meas: float(val) for meas,val in measDat }
                                              }
                                }
                    )
        
        # commit to database
        res = client.write_points(jsonLoad,database=group, **writeOpts)
        if res: pi_log('inserted {} points for group={} on topics={}'.format(len(jsonLoad), \
                       group, ",".join(usedTopics)))


# terminate kafka consumers
for _,con in consumers: con.close()
pi_log("Closed kafka consumers")

# terminate influxdb connection
client.close()
pi_log("Closed InfluxDB client")

pi_log("Successful completion")

