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
package org.hawkular.cmdgw.ws.command.feed;

import java.util.Collections;
import java.util.Map;

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.BasicMessageWithExtraData;
import org.hawkular.bus.common.BinaryData;
import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.producer.ProducerConnectionContext;
import org.hawkular.cmdgw.api.AddJdbcDriverResponse;
import org.hawkular.cmdgw.ws.Constants;
import org.hawkular.cmdgw.ws.MsgLogger;
import org.hawkular.cmdgw.ws.command.Command;
import org.hawkular.cmdgw.ws.command.CommandContext;

/**
 * A feed telling us it finished its application deployment attempt.
 */
public class AddJdbcDriverResponseCommand implements Command<AddJdbcDriverResponse, BasicMessage> {
    public static final Class<AddJdbcDriverResponse> REQUEST_CLASS = AddJdbcDriverResponse.class;

    @Override
    public BasicMessageWithExtraData<BasicMessage> execute(AddJdbcDriverResponse response, BinaryData binaryData,
            CommandContext context) throws Exception {

        MsgLogger.LOG.infof("Request completed: [%s]", response);

        // determine what UI client needs to be sent the message
        String uiClientId;

        // TODO: how do we get this?
        uiClientId = null;

        if (uiClientId == null) {
            // TODO: we need to know the ui client - but that hook isn't in yet so for now
            // the first UI that picks the message off the queue will get it
            // When we remove this, remove the HACK in UIClientListenerGenerator.addListeners(String) too
            try (ConnectionContextFactory ccf = new ConnectionContextFactory(context.getConnectionFactory())) {
                Endpoint endpoint = Constants.DEST_UICLIENT_ADD_JDBC_DRIVER_RESPONSE;
                ProducerConnectionContext pcc = ccf.createProducerConnectionContext(endpoint);
                new MessageProcessor().send(pcc, response);
            }
        } else {
            try (ConnectionContextFactory ccf = new ConnectionContextFactory(context.getConnectionFactory())) {
                Endpoint endpoint = Constants.DEST_UICLIENT_ADD_JDBC_DRIVER_RESPONSE;
                ProducerConnectionContext pcc = ccf.createProducerConnectionContext(endpoint);
                Map<String, String> uiClientIdHeader = Collections.singletonMap(Constants.HEADER_UICLIENTID,
                        uiClientId);
                new MessageProcessor().send(pcc, response, uiClientIdHeader);
            }
        }

        // nothing needs to be sent back to the feed
        return null;
    }
}
