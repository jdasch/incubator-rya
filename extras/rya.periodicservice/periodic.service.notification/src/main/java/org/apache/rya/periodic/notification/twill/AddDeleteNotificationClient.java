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

import java.util.UUID;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner.LiveInfo;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.yarn.YarnTwillRunnerService;

public class AddDeleteNotificationClient {

    public static void main(String[] args) {

        String zkStr = args[0];
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        TwillRunnerService runner = new YarnTwillRunnerService(yarnConfiguration, zkStr);
        runner.start();
        System.out.println("Looking for running instances of " + NotificationRunnable.class.getSimpleName());
        Iterable<TwillController> controllers = runner.lookup(NotificationRunnable.class.getSimpleName());
        System.out.println("Looking for any running instances");
        Iterable<LiveInfo> info = runner.lookupLive();
        for (LiveInfo i : info) {
            System.out.println("LiveInfo is " + i.getApplicationName());
        }

        TwillController cont = null;
        for (TwillController controller : controllers) {
            System.out.println("Found controller.");
            cont = controller;
            break;
        }

        if (cont != null) {
            String id = "notification_" + UUID.randomUUID().toString();
            System.out.println("Adding notification: " + id);
            cont.sendCommand(Command.Builder.of("add:" + id + ":1:seconds").build());

            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Deleting notification: " + id);
            cont.sendCommand(Command.Builder.of("delete:" + id).build());
        } else {
            System.exit(0);
        }

    }

}
