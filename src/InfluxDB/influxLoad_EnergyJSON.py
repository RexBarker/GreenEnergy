#######################################################################
# Loads date from JSON files into InfluxdB
# - load power sources
# - load climate sources 
#######################################################################

import re
import sys
import json
import os.path
import argparse
import logging
from glob import glob, iglob
from influxdb import InfluxDBClient
from datetime import datetime, timezone
import holidays

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

def check_config_file(config):
    config.get("connection") or \
        pe_log("Error: influxDB_Climate config missing \'connection\' definition")
    config["connection"].get(args.connection) or \
        pe_log(f"Error: influxDB_Climate config missing \'connection.{args.connection}\' definition")
    config["connection"][args.connection].get("host") or \
        pe_log(f"Error: influxDB_Climate config missing \'connection.{args.connection}.host\' definition")
    config["connection"][args.connection].get("port") or \
        pe_log(f"Error: influxDB_Climate config missing \'connection.{args.connection}.port\' definition")
    config.get("dbname") or \
        pe_log("Error: influxDB_Climate config missing \'dbname\' definition")
    config.get("measurements") or \
        pe_log("Error: influxDB_Climate config missing \'measurements\' definition")
    len(config["measurements"]) > 0 or \
        pe_log("Error: influxDB_Climate config missing measurement list definition")
    config.get("dataSource") or \
        pe_log("Error: influxDB_Climate config missing \'datasource\' definition")
    config.get("baseDir") or \
        pe_log("Error: influxDB_Climate config missing \'baseDir\' definition")

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


#----------------------------------------------------------------------

logging.basicConfig(filename="./influxDBLoad.log",filemode="a",
                    format='%(asctime)s %(message)s', datefmt='%Y.%m.%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger(__name__)

log.info("--- influxDB Load Energy JSON Sources Start ---")

parser = argparse.ArgumentParser()
parser.add_argument('-configfile',type=str,dest='configfile',required=False,default='config.json',
                    help='Required: configuration file for kafka producer (JSON format)')

parser.add_argument('-config',type=str,dest='config',required=True,
                    help='Required: configuration specification for database (JSON format)')

parser.add_argument('-connection',type=str,dest='connection',default='local',
                    help='Optional: connection to use for kafka producer [local, internal, external] (default: local))')

parser.add_argument('-removedb',dest='removedb',action='store_true',
                    help='Optional: drops previous database if exists')

parser.add_argument('-removemeasurement',dest='removemeasurement',action='store_true',
                    help='Optional: drops previous measurement table if exists') 

args = parser.parse_args()

# check the configuration file, see what we need to load
if not os.path.exists(args.configfile):  
    msg = f"config file={args.configfile} did not exist!"
    log.error(msg)
    print(msg)
    sys.exit()

with open(args.configfile,'r') as fp: 
    config = json.load(fp) 
    log.info(f"loaded config file={args.configfile}")
    
assert config.get(args.config), f"Error: invalid InfluxDB configuration specified: \'{args.config}\'" 
inp_config = config[args.config]
check_config_file(inp_config)

# check connection to InfluxDB
# default is internal connection
con = { 'host': inp_config['connection'][args.connection]['host'],
        'port': inp_config['connection'][args.connection]['port']}

# add extra setup if specified
if inp_config['connection'].get('setup'):
    con.update(inp_config['connection']['setup'])
else:
    con.update({ 'timeout': 5, 'retries': 3})

# add extra write options, if specified
writeOpts = inp_config.get('write options') or {}

# instatiate Influx client (does not necessarily tell of sucessful connection to database)
try:
    client = InfluxDBClient(**con) 
except:
    pe_log(f"Could not connect to InfluxDBClient with following arguments:{con}")

# determine existing databases
dbname = inp_config['dbname']
try:
    dbs = {d['name'] for d in client.get_list_database()}
except:
    pe_log(f"Could not connect to InfluxDBClient with following arguments:{con}")

pi_log(f"Successufl connection to InfluxDBClient with following arguments:{con}")

# remove old DB, if requested
if args.removedb:
    dbr = dbs.intersection({dbname})   # only 1 dbname supplied
    if dbr:
        print('\nThe following database will be removed:\n',list(dbr)[0],'\n')
        res = input('enter \'yes\' to proceed: ')
        if res == 'yes':
            pi_log(f"removing database:{list(dbr)[0]}" )
            try:
                client.drop_database(dbname)
                dbs = {d['name'] for d in client.get_list_database()}
            except:
                pe_log("An error occured while dropping database")
        else:
            pi_log("Aborted removing of topics")
    else:
        pi_log("No existing databases to drop")

if not dbs.intersection(dbname): client.create_database(dbname) 
client.switch_database(dbname)

# load data as measurements
measurements = list(inp_config['measurements'].keys()) if inp_config['use measurements'][0] == '*' else inp_config['use measurements'] 

# weights for export power, for tracking in the database (for holiday exports)
# when read from the json file:
#       exportWeights[dict(country)][dict(year)][list(month0)]
exportWeights = {}
exportWeightsFile = inp_config.get('export power weight file')
if exportWeightsFile and os.path.exists(exportWeightsFile):
    with open(exportWeightsFile,'r') as f:
        exportWeights = json.load(f)
    pi_log(f"Export weights specified from {exportWeightsFile}")
else:
    pw_log("No \'export power weight file\' config definition found, or file was missing...assuming equal weights")

######################
# Load measurements
######################
for meas in measurements:
    files = sorted( *[ glob(os.path.join(inp_config['baseDir'],f)) for f in inp_config['measurements'][meas]['files']])

    # determine which field values to select, default is all
    posFilterFields = [ re.compile('.*') ]
    negFilterFields = [] 
    renameFields = dict()
    if inp_config['measurements'][meas].get('field keys'):
        posPatterns = inp_config['measurements'][meas]['field keys'].get('include') or []
        negPatterns = inp_config['measurements'][meas]['field keys'].get('exclude') or []
        posFilterFields = [ re.compile(pat) for pat in posPatterns] if any(posPatterns) else [ re.compile('.*')] 
        negFilterFields = [ re.compile(pat) for pat in negPatterns] if any(negPatterns) else [] 
        renameFields = inp_config['measurements'][meas]['field keys'].get('rename') or dict() # field name replacement, if specified
    

    for f in files:
        with open(f,mode='r',encoding='ISO-8859-1') as fp:
            instr = fp.read().replace('\x00','0')
            data = json.loads(instr)
        
        # determine single column or double column measurements
        if isinstance(data[0]['key'],str):
            measList = [ re.sub(r'_*\(.*\)','',data[0]["y1AxisLabel"][0]["en"].replace(' ','_')),
                         re.sub(r'_*\(.*\)','',data[0]["y2AxisLabel"][0]["en"].replace(' ','_'))  ]
        else:
            measList = [meas]

        # sort points in the datasets within a file, make sure we have consistent time points 
        timePoints = { dtv[0] for dat in data for dtv in dat['values']}   # all time values in all sets
        for dat in data:
            tempPoints = { dtv[0] for dtv in dat['values']}
            timePoints.intersection_update(tempPoints)  # remove values which were not common

        timePoints =sorted(list(timePoints))

        # collect data for this measurement
        # filter for desired/undesired fields
        mydata = dict()
        for dat in data:
            if isinstance(dat['key'],str):                      
                field = dat['key']             # single value arrays 
            else:
                field = dat['key'][0]['en']    # multi-value arrays
            
            if     any([ pat.match(field) for pat in posFilterFields]) and \
               not any([ pat.match(field) for pat in negFilterFields]):
                if renameFields.get(field): field = renameFields[field]  # rename
                mydata[field] = { dt:val for dt,*val in dat['values']}
        
        for en,meas in enumerate(measList):
            jsonload = []
            for dt in timePoints:
                timestamp = datetime.fromtimestamp(dt/1000)
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

                # JSON payload formation
                jsonload.append(
                                    {
                                        "measurement": meas,
                                        "time": timestamp,
                                        "fields": { 
                                                    **{ "year": int(year), "month": int(month), 
                                                        "weekday": int(weekday), "holiday": float(holiday),
                                                        "hour": int(hour), "minute": int(minute) },
                                                    **{field: float(val[dt][en]) if val[dt][en] is not None else None for field,val in mydata.items()}
                                                  }
                                    }
                                )
        
            res = client.write_points(jsonload, **writeOpts)
            if res: pi_log('inserted {} points for {} fields for measurement={}, from file={}'.format(len(jsonload), \
                           len(mydata), meas,f))

if client is not None: client.close() 
pi_log("Successfully completed")