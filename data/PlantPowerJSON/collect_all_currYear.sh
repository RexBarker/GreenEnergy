#!/bin/bash
currYear=$(date +%Y)
currWeek=$(date +%V)
mybaseurl="https://www.energy-charts.de/power/"
mybasefile="week_$currYear"
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
     #echo "fake fetch $myurl"
     sleep 3
     #echo $myurl
   fi
  done

