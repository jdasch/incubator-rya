#!/bin/bash

PROJECT_HOME=$(dirname $(cd $(dirname $0) && pwd))
cd $PROJECT_HOME

HADOOP_CP=$(hadoop classpath)
echo $HADOOP_CP

$JAVA_HOME/bin/java -cp lib/*:$HADOOP_CP \
  -Dlogback.configurationFile=conf/logback.xml \
  org.apache.rya.periodic.notification.twill.HWAppJarRunner \
  zoo1,zoo2,zoo3,zoo4,zoo5 lib/rya.periodic.service.notification.twill-3.2.11-incubating-SNAPSHOT-bundle.jar 
