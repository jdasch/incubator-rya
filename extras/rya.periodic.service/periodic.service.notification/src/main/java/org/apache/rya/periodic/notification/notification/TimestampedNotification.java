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
package org.apache.rya.periodic.notification.notification;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimestampedNotification extends PeriodicNotification {

    private Date date;

    public TimestampedNotification(String id, long period, TimeUnit periodTimeUnit, long initialDelay) {
        super(id, period, periodTimeUnit, initialDelay);
        date = new Date();
    }
    
    public TimestampedNotification(PeriodicNotification notification) {
        super(notification);
        date = new Date();
    }

    public Date getTimestamp() {
        return date;
    }

    @Override
    public String toString() {
        return super.toString() + ";date=" + date;
    }

}
