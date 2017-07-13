#mvn clean package -Dhadoop.version=2.5.0-cdh5.3.3 -Daccumulo.version=1.6.2

mvn clean package
scp modules/distribution/target/fluo-1.0.0-incubating-bin.tar.gz destination:file/path/.


tar -xvzf fluo-1.0.0-incubating-bin.tar.gz
cp conf/examples/* conf/

vi conf/fluo.properties
# populate the following properties

#???
fluo.client.application.name=temp-app-1
fluo.client.zookeeper.connect=${fluo.client.accumulo.zookeepers}/fluo
fluo.client.accumulo.instance=<accumulo instance name>
fluo.client.accumulo.user=<accumulo user name>
fluo.client.accumulo.password=<accumulo user password>
fluo.client.accumulo.zookeepers=<zookeepers, ie: zoo1,zoo2,zoo3,zoo4,zoo5>
fluo.admin.hdfs.root=hdfs://hdfs-host-name:8020

# Add the cloudera repository to the ahz pom
vi lib/ahz/pom.xml
    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/content/repositories/releases/</url>
        </repository>
     </repositories>
./lib/fetch.sh ahz -Daccumulo.version=1.6.2 -Dhadoop.version=2.5.0-cdh5.3.3 -Dzookeeper.version=3.4.5-cdh5.3.3
./lib/fetch.sh ahz -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.0-cdh5.8.5 -Dzookeeper.version=3.4.5-cdh5.8.5
./lib/fetch.sh ahz -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.5 -Dzookeeper.version=3.4.10

 ./lib/fetch.sh extra
 
 
 # update the environment to use the locally downloaded libraries
 vi fluo-env.sh
 
 # uncomment
 setupClasspathFromLib
 
 # comment the following
 # setupClasspathFromSystem
 # test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop
 
 # you may still need to add the conf directory to the classpath 
 # run the command `hadoop classpath` to get an idea
 # typically /etc/hadoop/conf
 
 
 # alternatively use the following
 
 
 
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements.  See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.

# Sets HADOOP_PREFIX if it is not already set.  Please modify the
# export statement to use the correct directory.  Remove the test
# statement to override any previously set environment.

test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/usr

# The classpath for Fluo must be defined.  The Fluo tarball does not include
# jars for Accumulo, Zookeeper, or Hadoop.  This example env file offers two
# ways to setup the classpath with these jars.  Go to the end of the file for
# more info.

addToClasspath() 
{
  local dir=$1
  local filterRegex=$2

  if [ ! -d "$dir" ]; then
    echo "ERROR $dir does not exist or not a directory"
    exit 1
  fi

  for jar in $dir/*.jar; do
    if ! [[ $jar =~ $filterRegex ]]; then
       CLASSPATH="$CLASSPATH:$jar"
    fi
  done
}


# This function attemps to obtain Accumulo, Hadoop, and Zookeeper jars from the
# location where those dependencies are installed on the system.
setupClasspathFromSystem()
{
  test -z "$ACCUMULO_HOME" && ACCUMULO_HOME=/usr/lib/accumulo
  test -z "$ZOOKEEPER_HOME" && ZOOKEEPER_HOME=/usr/lib/zookeeper

  CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*"

  #any jars matching this pattern is excluded from classpath
  EXCLUDE_RE="(.*log4j.*)|(.*asm.*)|(.*guava.*)|(.*gson.*)"

  addToClasspath "$ACCUMULO_HOME/lib" $EXCLUDE_RE
  addToClasspath "$ZOOKEEPER_HOME" $EXCLUDE_RE
  addToClasspath "$ZOOKEEPER_HOME/lib" $EXCLUDE_RE
  
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/common" $EXCLUDE_RE;
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/common/lib" $EXCLUDE_RE;
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/hdfs" $EXCLUDE_RE;
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/hdfs/lib" $EXCLUDE_RE;
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/yarn" $EXCLUDE_RE;
#  addToClasspath "$HADOOP_PREFIX/share/hadoop/yarn/lib" $EXCLUDE_RE;
  
  # use the command `hadoop classpath` for inspriation
  CLASSPATH="$CLASSPATH:/etc/hadoop/conf"
  addToClasspath "/usr/lib/hadoop" $EXCLUDE_RE;
  addToClasspath "/usr/lib/hadoop/lib" $EXCLUDE_RE;
  addToClasspath "/usr/lib/hadoop-hdfs" $EXCLUDE_RE;
  addToClasspath "/usr/lib/hadoop-hdfs/lib" $EXCLUDE_RE;
  addToClasspath "/usr/lib/hadoop-yarn" $EXCLUDE_RE;
  addToClasspath "/usr/lib/hadoop-yarn/lib" $EXCLUDE_RE;
}


# This function obtains Accumulo, Hadoop, and Zookeeper jars from
# $FLUO_HOME/lib/ahz/. Before using this function, make sure you run
# `./lib/fetch.sh ahz` to download dependencies to this directory.
setupClasspathFromLib(){
  CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*:$FLUO_HOME/lib/ahz/*"
}

# Call one of the following functions to setup the classpath or write your own
# bash code to setup the classpath for Fluo. You must also run the command
# `./lib/fetch.sh extra` to download extra Fluo dependencies before using Fluo.

setupClasspathFromSystem
#setupClasspathFromLib
 
 
 
 
#create a new fluo app  (note, the app name is effectively ${rya_prefix}pcj_updater, where ${rya_prefix}==rya_)
bin/fluo new rya_pcj_updater
# edit the app config
vi apps/rya_pcj_updater/conf/fluo.properties

# under OBSERVER properties
fluo.observer.0=org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver
fluo.observer.1=org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver
fluo.observer.2=org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver
fluo.observer.3=org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver
fluo.observer.4=org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver,pcj.fluo.export.rya.enabled=true,pcj.fluo.export.rya.accumuloInstanceName=[ACCUMULO INSTANCE NAME],pcj.fluo.export.rya.zookeeperServers=[SEMICOLON DELIMITED LIST OF ZOOKEEPER HOSTNAMES],pcj.fluo.export.rya.exporterUsername=[ACCUMULO USERNAME],pcj.fluo.export.rya.exporterPassword=[ACCUMULO PASSWORD],pcj.fluo.export.rya.ryaInstanceName=[RYA_PREFIX_]
# or for kafka publishing
# fluo.observer.4=org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver,pcj.fluo.export.kafka.enabled=true,bootstrap.servers=node6:9092,key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer,value.serializer=org.apache.kafka.common.serialization.StringSerializer
 
# build and copy rya.pcj.fluo.app-VERSION-jar-with-dependencies.jar to 
mvn clean package -pl :rya.pcj.fluo.app -am -Dhadoop.version=2.5.0-cdh5.3.3 -Daccumulo.version=1.6.2
mvn clean package -pl :rya.pcj.fluo.app -am -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.0-cdh5.8.5 -Dzookeeper.version=3.4.5-cdh5.8.5
mvn clean package -pl :rya.pcj.fluo.app -am -Daccumulo.version=1.7.3 -Dhadoop.version=2.6.5 -Dzookeeper.version=3.4.10

cp rya.pcj.fluo.app-[VERSION]-jar-with-dependencies.jar apps/rya_pcj_updater/lib/.


# depending on hdfs permissions, you may need to run this command as the hdfs user.  especially if /fluo/lib doesn't exist.
# need to be able to write to hdfs://hostname/fluo/lib/*
bin/fluo init rya_pcj_updater

you may need to modify the hdfs permissions such that accumulo has access to this directory.   possibly as a privileged user  ie, sudo su hdfs
hadoop fs -chown accumulo /fluo/lib/*


Verify iterators...
config -t rya_pcj_updater -f iterators



bin/fluo start rya_pcj_updater



 
 