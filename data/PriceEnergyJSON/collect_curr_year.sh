#!/bin/bash
# arguments:  $1 : solar,wind,air (temperature & humidity)  
#             $2 : year
#             $3 : start month
#             $4 : end month
currYear=$(date +%Y)
currWeek=$(date +%V)
currMonth=$(date +%m)
##currQuery=$1
#currYear=$2
startWeek=1
#endMonth=$4

#https://www.energy-charts.de/price/week_2019_22.json
mybaseurl="https://www.energy-charts.de/price/"
mybasefile="week"
mybasefile+="_$currYear"
mybasefile+="_"

for (( m=startWeek; m<=currWeek; m++ ))
  do
   myfile=$mybasefile
   myfile+=$(printf "%02d" $m)
   myfile+=".json"
   if [ -f $myfile ]; then
     echo "file=$myfile exists"
   else
     echo "file=$myfile missing...retrieving"
     myurl=$mybaseurl
     myurl+=$myfile
     wget $myurl
     #echo "fake fetch"
     sleep 3
     #echo $myurl
   fi
  done

