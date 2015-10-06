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
package org.hawkular.cmdgw.command.bus;

import java.io.IOException;
import java.util.function.BiFunction;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Destroyed;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.websocket.Session;

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.BasicMessageWithExtraData;
import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.consumer.BasicMessageListener;
import org.hawkular.bus.common.consumer.ConsumerConnectionContext;
import org.hawkular.cmdgw.Constants;
import org.hawkular.cmdgw.command.ws.WsEndpoints;
import org.hawkular.cmdgw.command.ws.WsSessionListener;
import org.hawkular.cmdgw.command.ws.server.WebSocketHelper;
import org.hawkular.cmdgw.log.GatewayLoggers;
import org.hawkular.cmdgw.log.MsgLogger;

/**
 * A collection of listeners that are attached/removed to/from bus queues or topics as WebSocket clients connect and
 * disconnect.
 *
 * @author <a href="https://github.com/ppalaga">Peter Palaga</a>
 */
@ApplicationScoped
public class BusEndpointProcessors {

    /**
     * A {@link WsSessionListener} that adds the given {@link #busEndpointListener} to the given {@link #endpoint} on
     * {@link #sessionAdded()} and removes it on {@link #sessionRemoved()}.
     */
    private class BusWsSessionListener implements WsSessionListener {
        private final BasicMessageListener<BasicMessage> busEndpointListener;
        private ConsumerConnectionContext consumerConnectionContext;

        private final Endpoint endpoint;
        private final String messageSelector;
        private ConnectionContextFactory connectionContextFactory;

        public BusWsSessionListener(String selectorHeader, String selectorValue, Endpoint endpoint,
                BasicMessageListener<BasicMessage> busEndpointListener) {
            super();

            this.endpoint = endpoint;
            this.busEndpointListener = busEndpointListener;
            this.messageSelector = String.format("%s = '%s'", selectorHeader, selectorValue);

            log.debugf("Created [%s] for messageSelector [%s]", getClass().getName(), messageSelector);
        }

        /**
         * Adds the given {@link #busEndpointListener} to the given {@link #endpoint}.
         *
         * @see org.hawkular.cmdgw.command.ws.WsSessionListener#sessionAdded()
         */
        @Override
        public void sessionAdded() {
            log.debugf("Attaching [%s] with selector [%s] to bus endpoint [%s]",
                    busEndpointListener.getClass().getName(), messageSelector, endpoint);

            try {
                connectionContextFactory = new ConnectionContextFactory(true, connectionFactory);
                consumerConnectionContext = connectionContextFactory.createConsumerConnectionContext(endpoint,
                        messageSelector);
                new MessageProcessor().listen(consumerConnectionContext, busEndpointListener);
            } catch (JMSException e) {
                log.errorCouldNotAddBusEndpointListener(busEndpointListener.getClass().getName(), messageSelector,
                        endpoint.getName(), e);
            }
        }

        /**
         * Removes the given {@link #busEndpointListener} from the given {@link #endpoint}.
         *
         * @see org.hawkular.cmdgw.command.ws.WsSessionListener#sessionRemoved()
         */
        @Override
        public void sessionRemoved() {
            log.debugf("Removing [%s] with selector [%s] from bus endpoint [%s]",
                    busEndpointListener.getClass().getName(), messageSelector, endpoint);

            if (consumerConnectionContext != null) {
                try {
                    consumerConnectionContext.close();
                } catch (IOException e) {
                    log.errorCouldNotClose(consumerConnectionContext.getClass().getName(), messageSelector,
                            endpoint.getName(), e);
                }
            }

            if (connectionContextFactory != null) {
                try {
                    connectionContextFactory.close();
                } catch (Exception e) {
                    log.errorf(e, "Could not close connection context factory: " + getClass().getName());
                }
            }
        }

    }

    private static class FeedBusEndpointListener extends BasicMessageListener<BasicMessage> {

        private final Endpoint endpoint;
        private final String expectedFeedId;
        private final Session session;

        public FeedBusEndpointListener(Session session, String expectedFeedId, Endpoint endpoint) {
            super(FeedBusEndpointListener.class.getClassLoader());
            this.session = session;
            this.expectedFeedId = expectedFeedId;
            this.endpoint = endpoint;
        }

        @Override
        protected void onBasicMessage(BasicMessageWithExtraData<BasicMessage> messageWithData) {
            final BasicMessage basicMessage = messageWithData.getBasicMessage();
            try {
                log.debugf("Received message [%s] with binary data [%b] from endpoint [%s]",
                        basicMessage.getClass().getName(), messageWithData.getBinaryData() != null,
                        endpoint.getName());
                String foundFeedId = basicMessage.getHeaders().get(Constants.HEADER_FEEDID);
                if (foundFeedId == null) {
                    log.errorMessageWithoutFeedId(basicMessage.getClass().getName(), Constants.HEADER_FEEDID,
                            Constants.FEED_COMMAND_QUEUE.getName());
                } else if (!foundFeedId.equals(expectedFeedId)) {
                    log.errorListenerGotMessageWithUnexpectedHeaderValue(this.getClass().getName(),
                            basicMessage.getClass().getName(), Constants.HEADER_FEEDID, foundFeedId,
                            expectedFeedId, Constants.FEED_COMMAND_QUEUE.getName());
                } else {
                    new WebSocketHelper().sendSync(session, messageWithData);
                }
            } catch (Exception e) {
                log.errorCouldNotProcessBusMessage(basicMessage.getClass().getName(),
                        messageWithData.getBinaryData() != null, endpoint.getName(), e);
            }
        }

    }

    private static class UiClientBusEndpointListener
            extends org.hawkular.bus.common.consumer.BasicMessageListener<BasicMessage> {
        private final BusCommandContextFactory commandContextFactory;
        private final BusCommands commands;
        private final Endpoint endpoint;

        public UiClientBusEndpointListener(BusCommandContextFactory commandContextFactory, BusCommands commands,
                Endpoint endpoint) {
            super(UiClientBusEndpointListener.class.getClassLoader());
            this.commandContextFactory = commandContextFactory;
            this.commands = commands;
            this.endpoint = endpoint;
        }

        @Override
        protected void onBasicMessage(BasicMessageWithExtraData<BasicMessage> messageWithData) {
            final BasicMessage basicMessage = messageWithData.getBasicMessage();
            log.debugf("Received message [%s] with binary data [%b] from endpoint [%s]",
                    basicMessage.getClass().getName(), messageWithData.getBinaryData() != null,
                    endpoint.getName());
            try {
                @SuppressWarnings("unchecked")
                BusCommand<BasicMessage> command = (BusCommand<BasicMessage>) commands
                        .getCommand(messageWithData.getBasicMessage().getClass());
                BusCommandContext context = commandContextFactory.newCommandContext(null);
                command.execute(messageWithData, context);
            } catch (Exception e) {
                log.errorCouldNotProcessBusMessage(basicMessage.getClass().getName(),
                        messageWithData.getBinaryData() != null, endpoint.getName(), e);
            }
        }
    }

    private static final MsgLogger log = GatewayLoggers.getLogger(BusEndpointProcessors.class);

    @Inject
    private BusCommands busCommands;

    @Inject
    private BusCommandContextFactory commandContextFactory;

    @Resource(mappedName = Constants.CONNECTION_FACTORY_JNDI)
    private ConnectionFactory connectionFactory;

    private BiFunction<String, Session, WsSessionListener> feedSessionListenerProducer;
    private BiFunction<String, Session, WsSessionListener> uiClientSessionListenerProducer;

    @Inject
    private WsEndpoints wsEndpoints;

    public void destroy(@Observes @Destroyed(ApplicationScoped.class) Object ignore) {
        log.debugf("Destroying [%s]", this.getClass().getName());
        if (feedSessionListenerProducer != null) {
            wsEndpoints.getFeedSessions().removeWsSessionListenerProducer(feedSessionListenerProducer);
        }
        if (uiClientSessionListenerProducer != null) {
            wsEndpoints.getUiClientSessions().removeWsSessionListenerProducer(uiClientSessionListenerProducer);
        }
    }

    public void initialize(@Observes @Initialized(ApplicationScoped.class) Object ignore) {
        log.debugf("Initializing [%s]", this.getClass().getName());
        try {
            feedSessionListenerProducer = new BiFunction<String, Session, WsSessionListener>() {
                @Override
                public WsSessionListener apply(String key, Session session) {
                    final Endpoint endpoint = Constants.FEED_COMMAND_QUEUE;
                    BasicMessageListener<BasicMessage> busEndpointListener = new FeedBusEndpointListener(session, key,
                            endpoint);
                    return new BusWsSessionListener(Constants.HEADER_FEEDID, key, endpoint, busEndpointListener);
                }
            };
            wsEndpoints.getFeedSessions().addWsSessionListenerProducer(feedSessionListenerProducer);

            uiClientSessionListenerProducer = new BiFunction<String, Session, WsSessionListener>() {
                @Override
                public WsSessionListener apply(String key, Session session) {
                    final Endpoint endpoint = Constants.UI_COMMAND_QUEUE;
                    BasicMessageListener<BasicMessage> busEndpointListener = new UiClientBusEndpointListener(
                            commandContextFactory, busCommands, endpoint);
                    return new BusWsSessionListener(Constants.HEADER_UICLIENTID, key, endpoint, busEndpointListener);
                }
            };
            wsEndpoints.getUiClientSessions().addWsSessionListenerProducer(uiClientSessionListenerProducer);
        } catch (Exception e) {
            log.errorf(e, "Could not initialize " + getClass().getName());
        }

    }

}