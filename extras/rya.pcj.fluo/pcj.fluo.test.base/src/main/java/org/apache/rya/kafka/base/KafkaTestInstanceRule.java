package org.apache.rya.kafka.base;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.junit.rules.ExternalResource;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaTestInstanceRule extends ExternalResource {

    private static final EmbeddedKafkaInstance kafkaInstance = EmbeddedKafkaSingleton.getInstance();
    private String kafkaTopicName;
    private final boolean createTopic;

    /**
     * @param createTopic - If true, a topic shall be created for {@link #getKafkaTopicName()}. If false, no topics
     *            shall be created.
     */
    public KafkaTestInstanceRule(final boolean createTopic) {
        this.createTopic = createTopic;
    }

    /**
     * @return A unique topic name for this test execution. If multiple topics are required by a test, use this value as
     *         a prefix.
     */
    public String getKafkaTopicName() {
        if (kafkaTopicName == null) {
            throw new IllegalStateException("Cannot get Kafka Topic Name outside of a test execution.");
        }
        return kafkaTopicName;
    }

    @Override
    protected void before() throws Throwable {
        // Get the next kafka topic name.
        kafkaTopicName = kafkaInstance.getUniqueTopicName();

        if(createTopic) {
            // Setup Kafka.
            ZkUtils zkUtils = null;
            try {
                zkUtils = ZkUtils.apply(new ZkClient(kafkaInstance.getZookeeperConnect(), 30000, 30000, ZKStringSerializer$.MODULE$), false);
                AdminUtils.createTopic(zkUtils, kafkaTopicName, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
            } finally {
                if(zkUtils != null) {
                    zkUtils.close();
                }
            }
        }
    }

    @Override
    protected void after() {
        kafkaTopicName = null;
    }
}