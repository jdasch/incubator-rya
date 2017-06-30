/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.periodic.notification.application;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.I0Itec.zkclient.ZkClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.FluoITHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PeriodicQueryTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.apache.rya.periodic.notification.api.CreatePeriodicQuery;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class PeriodicNotificationApplicationIT extends RyaExportITBase {

    private PeriodicNotificationApplication app;
    private KafkaNotificationRegistrationClient registrar;
    private KafkaProducer<String, CommandNotification> producer;
    private Properties props;
    private Properties kafkaProps;
    PeriodicNotificationApplicationConfiguration conf;
    
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    
    @Before
    public void init() throws Exception {
        setUpKafka();
        props = getProps();
        conf = new PeriodicNotificationApplicationConfiguration(props);
        kafkaProps = getKafkaProperties(conf);
        app = PeriodicNotificationApplicationFactory.getPeriodicApplication(props);
        producer = new KafkaProducer<>(kafkaProps, new StringSerializer(), new CommandNotificationSerializer());
        registrar = new KafkaNotificationRegistrationClient(conf.getNotificationTopic(), producer);
    }
    
    private void setUpKafka() throws Exception {
        // Setup Kafka.
        zkServer = new EmbeddedZookeeper();
        final String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Brokersparql
        final Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        final KafkaConfig config = new KafkaConfig(brokerProps);
        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }
    
    @Test
    public void periodicApplicationTestWithGroupBy() throws Exception {

        String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n
        
        //make data
        int periodMult = 15;
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        //Sleep until current time aligns nicely with period to make
        //results more predictable
        while(System.currentTimeMillis() % (periodMult*1000) > 500);
        ZonedDateTime time = ZonedDateTime.now();
        System.out.println("Now: " + time.toInstant().toEpochMilli());

        ZonedDateTime zTime1 = time.minusSeconds(2*periodMult);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusSeconds(periodMult);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusSeconds(periodMult);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")));
        
        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            Connector connector = ConfigUtils.getConnector(conf);
            PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            String id = periodicQuery.createQueryAndRegisterWithKafka(sparql, registrar);
            addData(statements);
            app.start();
//            
            Multimap<Long, BindingSet> expected = HashMultimap.create();
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                long end = System.currentTimeMillis() + 4*periodMult*1000;
                long lastBinId = 0L;
                long binId = 0L;
                List<Long> ids = new ArrayList<>();
                while (System.currentTimeMillis() < end) {
                    ConsumerRecords<String, BindingSet> records = consumer.poll(periodMult*1000);
                    for(ConsumerRecord<String, BindingSet> record: records){
                        BindingSet result = record.value();
                        binId = Long.parseLong(result.getBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID).getValue().stringValue());
                        if(lastBinId != binId) {
                            lastBinId = binId;
                            ids.add(binId);
                        }
                        expected.put(binId, result);
                    }
                }
                
                Assert.assertEquals(3, expected.asMap().size());
                int i = 0;
                for(Long ident: ids) {
                    Assert.assertEquals(3 - i, expected.get(ident).size());
                    i++;
                }
            }
            
            Set<BindingSet> expectedResults = new HashSet<>();
            try (CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty())) {
                results.forEachRemaining(x -> expectedResults.add(x));
                Assert.assertEquals(0, expectedResults.size());
            }
        }
    }
    
    
    @Test
    public void periodicApplicationTest() throws Exception {

        String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } "; // n
        
        //make data
        int periodMult = 15;
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        //Sleep until current time aligns nicely with period to make
        //results more predictable
        while(System.currentTimeMillis() % (periodMult*1000) > 500);
        ZonedDateTime time = ZonedDateTime.now();
        System.out.println("Now: " + time.toInstant().toEpochMilli());

        ZonedDateTime zTime1 = time.minusSeconds(2*periodMult);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusSeconds(periodMult);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusSeconds(periodMult);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")));
        
        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            Connector connector = ConfigUtils.getConnector(conf);
            PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            String id = periodicQuery.createQueryAndRegisterWithKafka(sparql, registrar);
            addData(statements);
            app.start();
//            
            Multimap<Long, BindingSet> expected = HashMultimap.create();
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                long end = System.currentTimeMillis() + 4*periodMult*1000;
                long lastBinId = 0L;
                long binId = 0L;
                List<Long> ids = new ArrayList<>();
                while (System.currentTimeMillis() < end) {
                    ConsumerRecords<String, BindingSet> records = consumer.poll(periodMult*1000);
                    records.forEach(x -> System.out.println(x.value()));
                    for(ConsumerRecord<String, BindingSet> record: records){
                        BindingSet result = record.value();
                        binId = Long.parseLong(result.getBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID).getValue().stringValue());
                        if(lastBinId != binId) {
                            lastBinId = binId;
                            ids.add(binId);
                        }
                        expected.put(binId, result);
                    }
                }
                
                Assert.assertEquals(3, expected.asMap().size());
                int i = 0;
                for(Long ident: ids) {
                    Assert.assertEquals(1, expected.get(ident).size());
                    BindingSet bs = expected.get(ident).iterator().next();
                    Value val = bs.getValue("total");
                    int total = Integer.parseInt(val.stringValue());
                    Assert.assertEquals(3-i, total);
                    i++;
                }
            }
            
            String tableName = new PeriodicQueryTableNameFactory().makeTableName(getRyaInstanceName(), id);
            Authorizations auths = super.getAccumuloConnector().securityOperations().getUserAuthorizations(getUsername());
            Scanner scanner = super.getAccumuloConnector().createScanner(tableName, auths);
            for(Map.Entry<Key, org.apache.accumulo.core.data.Value> entry: scanner) {
                AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
                System.out.println(entry.getKey());
                System.out.println(entry.getKey().getRow());
                System.out.println(converter.convert(entry.getKey().getRow().getBytes(), new VariableOrder(IncrementalUpdateConstants.PERIODIC_BIN_ID)));
            }
            
            Set<BindingSet> expectedResults = new HashSet<>();
            try (CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty())) {
                results.forEachRemaining(x -> expectedResults.add(x));
                Assert.assertEquals(0, expectedResults.size());
            }
        }

    }
    
    
    @After
    public void shutdown() {
        registrar.close();
        app.stop();
        teardownKafka();
    }
    
    private void teardownKafka() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }
    
    private void addData(Collection<Statement> statements) throws DatatypeConfigurationException {
        // add statements to Fluo
        try (FluoClient fluo = new FluoClientImpl(getFluoConfiguration())) {
            InsertTriples inserter = new InsertTriples();
            statements.forEach(x -> inserter.insert(fluo, RdfToRyaConversions.convertStatement(x)));
            getMiniFluo().waitForObservers();
            FluoITHelper.printFluoTable(fluo);
        }

    }

    private Properties getKafkaProperties(PeriodicNotificationApplicationConfiguration conf) { 
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getBootStrapServers());
        kafkaProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, conf.getNotificationClientId());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, conf.getNotificationGroupId());
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProps;
    }

    
    private Properties getProps() throws IOException {
        
        Properties props = new Properties();
        try(InputStream in = new FileInputStream("src/test/resources/notification.properties")) {
            props.load(in);
        } 
        
        FluoConfiguration fluoConf = getFluoConfiguration();
        props.setProperty("accumulo.user", getUsername());
        props.setProperty("accumulo.password", getPassword());
        props.setProperty("accumulo.instance", getMiniAccumuloCluster().getInstanceName());
        props.setProperty("accumulo.zookeepers", getMiniAccumuloCluster().getZooKeepers());
        props.setProperty("accumulo.rya.prefix", getRyaInstanceName());
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_APP_NAME, fluoConf.getApplicationName());
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_TABLE_NAME, fluoConf.getAccumuloTable());
        return props;
    }

}
