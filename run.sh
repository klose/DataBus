#!/bin/sh
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=` pwd .`
CLASSPATH=$CLASSPATH:${parent_path}/lib/zmq.jar
CLASSPATH=$CLASSPATH:${parent_path}/classes
JAVA_LIBRARY_PATH=/home/jiangbing/MapReduceMessage/lib
echo $CLASSPATH
java -server -Xmx4096m -Xms128m -cp $CLASSPATH -Djava.library.path=${JAVA_LIBRARY_PATH} $@



