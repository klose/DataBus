#!/bin/sh 
files=`find -name *.java`
for i in  $files 
do
	sed -i 's#cn.ict#com#g' $i
done
