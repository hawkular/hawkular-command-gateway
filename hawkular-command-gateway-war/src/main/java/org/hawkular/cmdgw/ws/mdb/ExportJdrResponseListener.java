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
package org.hawkular.cmdgw.ws.mdb;

import java.util.concurrent.ExecutorService;

import javax.websocket.Session;

import org.hawkular.bus.common.BasicMessageWithExtraData;
import org.hawkular.bus.common.BinaryData;
import org.hawkular.bus.common.consumer.BasicMessageListener;
import org.hawkular.cmdgw.api.ApiDeserializer;
import org.hawkular.cmdgw.api.ExportJdrResponse;
import org.hawkular.cmdgw.ws.Constants;
import org.hawkular.cmdgw.ws.MsgLogger;
import org.hawkular.cmdgw.ws.WebSocketHelper;
import org.hawkular.cmdgw.ws.server.ConnectedUIClients;

/**
 * @author Juraci Paixão Kröhling
 */
public class ExportJdrResponseListener extends BasicMessageListener<ExportJdrResponse> {
    private ConnectedUIClients connectedUIClients;
    private final ExecutorService threadPool;

    public ExportJdrResponseListener(ConnectedUIClients connectedUIClients, ExecutorService threadPool) {
        this.connectedUIClients = connectedUIClients;
        this.threadPool = threadPool;
    }

    protected void onBasicMessage(BasicMessageWithExtraData<ExportJdrResponse> responseWithData) {
        try {
            ExportJdrResponse response = responseWithData.getBasicMessage();

            String uiClientId = response.getHeaders().get(Constants.HEADER_UICLIENTID);
            if (uiClientId == null) {
                // TODO: for now, just send it to all UI clients on our server (we don't really want this behavior)
                //       we really want to those this exception since in the future the header must be there
                //throw new IllegalArgumentException("Missing header: " + Constants.HEADER_UICLIENTID);
                MsgLogger.LOG.warnf("HACK: Telling ALL UI that export JDR on resource [%s] resulted in [%s]",
                        response.getResourcePath(), response.getStatus());
                BinaryData dataToSend = ApiDeserializer.toHawkularFormat(response, responseWithData.getBinaryData());
                for (Session session : connectedUIClients.getAllSessions()) {
                    new WebSocketHelper().sendBinaryAsync(session, dataToSend, threadPool);
                }
                return;
            }

            // we are assuming the UI client ID *is* the session ID
            Session session = connectedUIClients.getSessionBySessionId(uiClientId);
            if (session == null) {
                return; // we don't have the UI client, this message isn't for us
            }

            MsgLogger.LOG.infof("Telling UI client [%s] that export JDR on resource [%s] resulted in [%s]",
                    uiClientId, response.getResourcePath(), response.getStatus());

            // send the request to the UI client
            BinaryData dataToSend = ApiDeserializer.toHawkularFormat(response, responseWithData.getBinaryData());
            new WebSocketHelper().sendBinaryAsync(session, dataToSend, threadPool);
            return;

        } catch (Exception e) {
            // catch all exceptions and just log the error to let us auto-ack the message anyway
            MsgLogger.LOG.errorCannotProcessExportJdrResponseMessage(e);
        }
    }
}
