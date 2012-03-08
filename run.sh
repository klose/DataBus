#!/bin/sh
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=` pwd .`
CLASSPATH=$CLASSPATH:${parent_path}/lib/zmq.jar
CLASSPATH=$CLASSPATH:${parent_path}/classes
JAVA_LIBRARY_PATH=/usr/local/lib:/usr/local/share/java/
echo $CLASSPATH
java -server -Xmx1024m -Xms128m -cp $CLASSPATH -Djava.library.path=${JAVA_LIBRARY_PATH} $@



