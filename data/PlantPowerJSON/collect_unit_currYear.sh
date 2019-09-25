#!/bin/bash
currYear=$(date +%Y)
currWeek=$(date +%V)
mybaseurl="https://www.energy-charts.de/power_unit/"
mybasefile="week_$1"
mybasefile+="_$currYear"
mybasefile+="_"


for (( w=1; w<=currWeek; w++ ))
  do
   myfile=$mybasefile
   myfile+=$(printf "%02d" $w)
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

