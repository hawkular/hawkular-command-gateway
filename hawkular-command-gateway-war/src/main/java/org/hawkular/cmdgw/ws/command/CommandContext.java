/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.cmdgw.ws.command;

import javax.jms.ConnectionFactory;
import javax.websocket.Session;

import org.hawkular.cmdgw.ws.server.ConnectedFeeds;
import org.hawkular.cmdgw.ws.server.ConnectedUIClients;

public class CommandContext {
    private final ConnectedFeeds connectedFeeds;
    private final ConnectedUIClients connectedUIClients;
    private final ConnectionFactory connectionFactory;
    private final Session session;

    public CommandContext(ConnectedFeeds f, ConnectedUIClients ui, ConnectionFactory cf, Session session) {
        this.connectedFeeds = f;
        this.connectedUIClients = ui;
        this.connectionFactory = cf;
        this.session = session;
    }

    public ConnectedFeeds getConnectedFeeds() {
        return connectedFeeds;
    }

    public ConnectedUIClients getConnectedUIClients() {
        return connectedUIClients;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public Session getSession() {
        return session;
    }
}