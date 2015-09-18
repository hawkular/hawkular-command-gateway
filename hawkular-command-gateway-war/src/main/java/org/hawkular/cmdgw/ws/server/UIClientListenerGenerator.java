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
package org.hawkular.cmdgw.ws.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.persistence.PostRemove;

import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.consumer.ConsumerConnectionContext;
import org.hawkular.cmdgw.ws.Constants;
import org.hawkular.cmdgw.ws.MsgLogger;
import org.hawkular.cmdgw.ws.mdb.AddDatasourceResponseListener;
import org.hawkular.cmdgw.ws.mdb.AddJdbcDriverResponseListener;
import org.hawkular.cmdgw.ws.mdb.DeployApplicationResponseListener;
import org.hawkular.cmdgw.ws.mdb.ExecuteOperationResponseListener;

@Startup
@Singleton
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class UIClientListenerGenerator {
    @EJB
    private ConnectedUIClients connectedUIClients;

    @Resource(mappedName = Constants.CONNECTION_FACTORY_JNDI)
    private ConnectionFactory connectionFactory;

    private Map<String, ConnectionContextFactory> connContextFactories;
    private Map<String, List<ConsumerConnectionContext>> consumerContexts;

    @PostConstruct
    public void initialize() throws Exception {
        if (this.connectionFactory == null) {
            MsgLogger.LOG.warnf("Injection of ConnectionFactory is not working - looking it up explicitly");
            InitialContext ctx = new InitialContext();
            this.connectionFactory = (ConnectionFactory) ctx.lookup(Constants.CONNECTION_FACTORY_JNDI);
        } else {
            MsgLogger.LOG.warnf("Injection of ConnectionFactory works - you can remove the hack");
        }

        connContextFactories = new HashMap<>();
        consumerContexts = new HashMap<>();
    }

    @PostRemove
    public void shutdown() throws Exception {
        if (connContextFactories != null) {
            for (String uiClientId : this.connContextFactories.keySet()) {
                removeListeners(uiClientId);
            }
        }
    }

    /**
     * @return the connection factory this object will use when connecting to the messaging system.
     */
    public ConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }

    public void addListeners(String uiClientId) throws Exception {
        removeListeners(uiClientId); // make sure any old ones aren't still hanging around
        ConnectionContextFactory ccf = new ConnectionContextFactory(true, connectionFactory);
        this.connContextFactories.put(uiClientId, ccf);
        List<ConsumerConnectionContext> contextList = new ArrayList<ConsumerConnectionContext>();
        this.consumerContexts.put(uiClientId, contextList);

        MsgLogger.LOG.infoAddingListenersForUIClient(uiClientId);

        MessageProcessor messageProcessor = new MessageProcessor();
        String messageSelector = String.format("%s = '%s'", Constants.HEADER_UICLIENTID, uiClientId);
        Endpoint endpoint;
        ConsumerConnectionContext ccc;

        // add additional listeners for UI clients - the listeners only get messages destined for their UI client ID.
        // As we introduce new messages the UI can receive, add them here.

        // TODO TEMP HACK - RIGHT NOW, WE AREN'T PASSING THE HEADER SO USE null SELECTOR
        //                  When we start putting the client ID in the message header, remove this =null statement
        messageSelector = null;

        endpoint = Constants.DEST_UICLIENT_EXECUTE_OP_RESPONSE;
        ccc = ccf.createConsumerConnectionContext(endpoint, messageSelector);
        messageProcessor.listen(ccc, new ExecuteOperationResponseListener(connectedUIClients));
        contextList.add(ccc);

        endpoint = Constants.DEST_UICLIENT_DEPLOY_APPLICATION_RESPONSE;
        ccc = ccf.createConsumerConnectionContext(endpoint, messageSelector);
        messageProcessor.listen(ccc, new DeployApplicationResponseListener(connectedUIClients));
        contextList.add(ccc);

        endpoint = Constants.DEST_UICLIENT_ADD_JDBC_DRIVER_RESPONSE;
        ccc = ccf.createConsumerConnectionContext(endpoint, messageSelector);
        messageProcessor.listen(ccc, new AddJdbcDriverResponseListener(connectedUIClients));
        contextList.add(ccc);

        endpoint = Constants.DEST_UICLIENT_ADD_DATASOURCE_RESPONSE;
        ccc = ccf.createConsumerConnectionContext(endpoint, messageSelector);
        messageProcessor.listen(ccc, new AddDatasourceResponseListener(connectedUIClients));
        contextList.add(ccc);

        return;
    }

    public void removeListeners(String uiClientId) {
        // When we created the factory, we had it reuse its one connection for all contexts.
        // When closing the factory, it then closes that connection which (should) close all
        // consumers the factory created. But this doesn't seem to work, so I'm closing all contexts first
        // then the factory.

        List<ConsumerConnectionContext> contextList = this.consumerContexts.remove(uiClientId);
        ConnectionContextFactory factory = this.connContextFactories.remove(uiClientId);

        if (contextList != null) {
            for (ConsumerConnectionContext context : contextList) {
                try {
                    context.close();
                } catch (Exception e) {
                    MsgLogger.LOG.errorFailedClosingConsumerContext(e);
                }
            }
        }

        if (factory != null) {
            try {
                MsgLogger.LOG.infoRemovingListenersForUIClient(uiClientId);
                factory.close();
            } catch (Exception e) {
                MsgLogger.LOG.errorFailedRemovingListenersForUIClient(uiClientId, e);
            }
        }
    }
}
