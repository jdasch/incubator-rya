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
package org.apache.rya.periodic.notification.twill;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationRunnable extends AbstractTwillRunnable {

    private ScheduledExecutorService service;
    private static final Logger LOG = LoggerFactory.getLogger(NotificationRunnable.class);
    private Map<String, ScheduledFuture<?>> serviceMap = new HashMap<>();

    @Override
    public void run() {
        service = Executors.newScheduledThreadPool(1);
        String id = "Notification_" + UUID.randomUUID().toString();
        ScheduledFuture<?> future = service.scheduleAtFixedRate(new Notification(id), 0, 5, TimeUnit.MINUTES);
        serviceMap.put(id, future);

        try {
            service.awaitTermination(365, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        this.stop();
    }

    @Override
    public void handleCommand(Command arg0) throws Exception {
        String command = arg0.getCommand();
        String[] commandArray = command.split(":");
        String commandString = commandArray[0].trim().toLowerCase();
        String notificationId = commandArray[1].trim();

        switch (commandString) {
        case "add":
            addService(commandArray);
            break;
        case "delete":
            ScheduledFuture<?> deleteFuture = serviceMap.get(notificationId);
            if (deleteFuture != null) {
                deleteFuture.cancel(true);
            }
        default:
            break;

        }

    }

    private void addService(String[] commandArray) {
        String notificationId = commandArray[1].trim();
        long period = Integer.parseInt(commandArray[2]);
        String unit = commandArray[3].trim().toLowerCase();
        TimeUnit tu = null;

        switch (unit) {
        case "seconds":
            tu = TimeUnit.SECONDS;
            break;
        case "minutes":
            tu = TimeUnit.MINUTES;
            break;
        case "hours":
            tu = TimeUnit.HOURS;
            break;
        default:
            break;
        }

        ScheduledFuture<?> future = service.scheduleAtFixedRate(new Notification(notificationId), 0, period, tu);
        serviceMap.put(notificationId, future);
    }

    @Override
    public void stop() {
        service.shutdown();
        LOG.info("Service Executor Shutdown has been called.  Terminating NotificationRunnable");
    }

    class Notification implements Runnable {

        private String notificationId;

        public Notification(String notificationId) {
            this.notificationId = notificationId;
        }

        public void run() {
            LOG.info("THIS IS A PERIODIC NOTIFICATION: " + notificationId + " : " + new Date());
        }

    }

}
