#!/bin/bash
mybaseurl="https://www.energy-charts.de/power_unit/"
mybasefile="week_$1"
mybasefile+="_$2"
mybasefile+="_"

for w in {1..9}
  do
   myfile=$mybasefile
   myfile+="0$w"
   myfile+=".json"
   if [ -f $myfile ]; then
     echo "file=$myfile exists"
   else
     echo "file=$myfile missing...retrieving"
     myurl=$mybaseurl
     myurl+=$myfile
     wget $myurl
     sleep 3
     #echo $myurl
   fi
  done

for w in {10..52}
  do
   myfile=$mybasefile
   myfile+="$w"
   myfile+=".json"
   if [ -f $myfile ]; then
     echo "file=$myfile exists"
   else
     echo "file=$myfile missing...retrieving"
     myurl=$mybaseurl
     myurl+=$myfile
     wget $myurl
     sleep 3
     #echo $myurl
   fi
  done


