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

import org.apache.rya.periodic.notification.api.Notification;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class CommandNotification implements Notification {

    private Notification notification;
    private Command command;

    public enum Command {
        ADD, DELETE
    };

    public CommandNotification(Command command, Notification notification) {
        Preconditions.checkNotNull(notification);
        Preconditions.checkNotNull(command);
        this.command = command;
        this.notification = notification;
    }

    @Override
    public String getId() {
        return notification.getId();
    }

    public Notification getNotification() {
        return this.notification;
    }

    public Command getCommand() {
        return this.command;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof CommandNotification) {
            CommandNotification cn = (CommandNotification) other;
            return Objects.equal(this.command, cn.command) && Objects.equal(this.notification, cn.notification);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Objects.hashCode(command);
        result = 31 * result + Objects.hashCode(notification);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("command").append("=").append(command.toString()).append(";")
                .append(notification.toString()).toString();
    }

}
