# Collect weather data for predetermined cities
# these are the corresponding cities with:
#  - wind
#  - solar
#  - weather data exists on openweather api

import json
import requests
import pandas as pd
from time import time, sleep
from datetime import datetime

def writeData(title,timetext,data):
    histfile = f'{title}_{timetext}.json'
    with open(histfile,'a') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

long_wait = 15*60  # 15 * 60 seconds
short_wait = 5   # 5 seconds

currentWeatherUrl='http://api.openweathermap.org/data/2.5/weather?q={}&APPID=asdfasdfasdgasdfasa_get_your_own_key'
forecastWeatherUrl='http://api.openweathermap.org/data/2.5/forecast?q={}&APPID=asdfasdfasdgasdfasa_get_your_own_key'

citiesUTF8  = ('Bremen','Chemnitz','Fichtelberg','Fürstenzell','Görlitz','Hamburg-Fuhlsbüttel','Hohenpeißenberg',\
               'Konstanz','Lindenberg','Norderney','Potsdam','Rostock-Warnemünde','Saarbrücken-Ensheim',\
               'Schleswig','Seehausen','Stuttgart (Schnarrenberg)','Trier-Petrisberg','Würzburg')

citiesASCII = ('Bremen','Chemnitz','Fichtelberg','Furstenzell','Gorlitz','Hamburg',            'Hohenpeissenberg',\
               'Konstanz','Lindenberg','Norderney','Potsdam','Rostock'           ,'Saarbrucken',\
               'Schleswig','Seehausen','Stuttgart',                'Trier',            'Wuerzburg')

cities = citiesASCII

while True:
    currData = {}
    forecastData = {}
    timetext = datetime.fromtimestamp(time()).strftime('%Y%m%d_%H%M%S')
    for city in cities:
        response = requests.get(currentWeatherUrl.format(city + ',DE'))
        responseOut = {}
        if response:
            responseOut = response.json()
            timenow = datetime.fromtimestamp(time()).strftime('%H:%M:%S')
            print(f'Retrieved current weather for city={city} at {timenow}')
        else:
            print(f'Could not retreive current weather for city={city}')  
        
        currData[city] = responseOut
        sleep(short_wait)
        
        response = requests.get(forecastWeatherUrl.format(city + ',DE'))
        responseOut = {}
        if response:
            responseOut = response.json()
            timenow = datetime.fromtimestamp(time()).strftime('%H:%M:%S')
            print(f'Retrieved forecast weather for city={city} at {timenow}')
        else:
            print(f'Could not retreive current weather for city={city}')  
        
        forecastData[city] = responseOut
        sleep(short_wait)
    
    # dump data
    writeData('curr',timetext,currData)
    writeData('fore',timetext,forecastData)
    
    # wait till next 
    sleep(long_wait - 2*len(cities)*short_wait)
