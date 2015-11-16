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
package org.hawkular.cmdgw.command.ws.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import javax.inject.Inject;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.OnMessage;
import javax.websocket.Session;

import org.hawkular.accounts.websocket.Authenticator;
import org.hawkular.accounts.websocket.WebsocketAuthenticationException;
import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.BasicMessageWithExtraData;
import org.hawkular.cmdgw.NoCommandForMessageException;
import org.hawkular.cmdgw.api.ApiDeserializer;
import org.hawkular.cmdgw.api.AuthMessage;
import org.hawkular.cmdgw.api.Authentication;
import org.hawkular.cmdgw.api.GenericErrorResponse;
import org.hawkular.cmdgw.api.GenericErrorResponseBuilder;
import org.hawkular.cmdgw.api.UiSessionOrigin;
import org.hawkular.cmdgw.command.bus.BusEndpointProcessors;
import org.hawkular.cmdgw.command.ws.WsCommand;
import org.hawkular.cmdgw.command.ws.WsCommandContext;
import org.hawkular.cmdgw.command.ws.WsCommandContextFactory;
import org.hawkular.cmdgw.command.ws.WsCommands;
import org.hawkular.cmdgw.command.ws.WsEndpoints;
import org.hawkular.cmdgw.log.GatewayLoggers;
import org.hawkular.cmdgw.log.MsgLogger;

/**
 * A common parent for {@link FeedWebSocket} and {@link UIClientWebSocket}.
 *
 * @author <a href="https://github.com/ppalaga">Peter Palaga</a>
 */
public abstract class AbstractGatewayWebSocket {
    private static final MsgLogger log = GatewayLoggers.getLogger(AbstractGatewayWebSocket.class);

    @Inject
    protected Authenticator authenticator;

    @Inject
    protected WsCommandContextFactory commandContextFactory;

    /**
     * This is the actual URL context of the websocket endpoint. For example, something
     * like "ui/ws" for a UI client websocket endpoint or "feed/{feedId}" for a feed websocket endpoint.
     */
    protected final String endpoint;

    @Inject
    protected WsCommands wsCommands;

    /**
     * A container that holds all known and currently active websocket clients (UI and feed clients).
     */
    @Inject
    protected WsEndpoints wsEndpoints;

    /**
     * These perform some additional processing when UI clients or feeds connect and disconnect.
     * This performs such tasks as adding and removing bus listeners that will help process
     * message flow for UI clients and feeds.
     */
    @Inject
    protected BusEndpointProcessors busEndpointProcessors;

    public AbstractGatewayWebSocket(String endpoint) {
        super();
        this.endpoint = endpoint;
    }

    /**
     * This makes sure the message is properly authenticated. If the message is not authenticated
     * with the appropriate credentials, an exception is thrown. Otherwise, this returns silently,
     * allowing the caller to continue.
     *
     * @param basicMessage the message (including its authentication credentials) to be authenticated
     * @param session the session that sent the message
     * @throws WebsocketAuthenticationException if the authentication check fails
     */
    protected void authenticate(BasicMessage basicMessage, Session session) throws WebsocketAuthenticationException {
        // if no authentication information is passed in the message, we will still ask Hawkular Accounts
        // to authenticate our session. This is to ensure any previous credentials/token that was authenticated
        // in the past is still valid now.
        log.tracef("About to authenticate message [%s] from WebSocket session [%s] of [%s]",
                basicMessage.getClass().getName(), session.getId(), endpoint);

        String username = null;
        String password = null;
        String token = null;
        String persona = null;

        if (basicMessage instanceof AuthMessage) {
            AuthMessage authMessage = (AuthMessage) basicMessage;
            Authentication auth = authMessage.getAuthentication();
            if (auth != null) {
                username = auth.getUsername();
                password = auth.getPassword();
                token = auth.getToken();
                persona = auth.getPersona();

                // make sure we clear the auth message - if we forward it we don't want the creds going along with it
                authMessage.setAuthentication(null);
            }
        }

        try {
            // note that if both username and token are provided, we authenticate using the token
            boolean hasToken = (token != null && !token.isEmpty());
            if (hasToken) {
                log.tracef("authenticating token [%s/%s], session=[%s]", token, persona, session.getId());
                authenticator.authenticateWithToken(token, persona, session);
            } else {
                log.tracef("authenticating user [%s/%s/%s], session=[%s]", username, password, persona,
                        session.getId());
                authenticator.authenticateWithCredentials(username, password, persona, session);
            }
        } catch (WebsocketAuthenticationException wae) {
            throw wae;
        } catch (Exception e) {
            throw new WebsocketAuthenticationException("Unauthorized!", e);
        }

        return; // authentication successful
    }

    protected void handleRequest(Session session, BasicMessageWithExtraData<BasicMessage> requestWithBinary)
            throws WebsocketAuthenticationException, NoCommandForMessageException, Exception {
        BasicMessage request = requestWithBinary.getBasicMessage();

        if (request instanceof UiSessionOrigin) {
            /* do not trust the sessionId provided by the client */
            log.tracef("[%s] is an instance of [%s]", request.getClass().getName(), UiSessionOrigin.class.getName());
            ((UiSessionOrigin) request).setSenderSessionId(session.getId());
        }

        // make sure the user is authenticated
        authenticate(request, session);

        Class<BasicMessage> requestClass = (Class<BasicMessage>) request.getClass();
        Collection<WsCommand<BasicMessage>> commands = wsCommands.getCommands(requestClass);
        for (WsCommand<BasicMessage> command : commands) {
            log.debugf("About to execute command [%s] on message [%s] in session [%s] of [%s]", command.getClass(),
                    requestClass.getName(), session.getId(), endpoint);
            WsCommandContext context = commandContextFactory.newCommandContext(session);
            command.execute(requestWithBinary, context); // NOTE: only 1 of the collection can read the binary stream
        }
    }

    /**
     * When a binary message is received from a WebSocket client, this method will lookup the {@link WsCommand} for the
     * given request class and execute it.
     *
     * @param binaryDataStream contains the JSON request and additional binary data
     * @param session the client session making the request
     */
    @OnMessage
    public void onBinaryMessage(InputStream binaryDataStream, Session session) {
        String requestClassName = "?";
        try {
            // parse the JSON and get its message POJO, including any additional binary data being streamed
            BasicMessageWithExtraData<BasicMessage> reqWithData = new ApiDeserializer().deserialize(binaryDataStream);
            BasicMessage request = reqWithData.getBasicMessage();
            requestClassName = request.getClass().getName();
            log.infoReceivedBinaryData(requestClassName, session.getId(), endpoint);

            handleRequest(session, reqWithData);

        } catch (WebsocketAuthenticationException wae) {
            try {
                session.close(new CloseReason(CloseCodes.VIOLATED_POLICY, wae.getLocalizedMessage()));
            } catch (IOException ioe) {
                log.errorCloseSessionAfterAuthFailure(ioe, session.getId(), endpoint, requestClassName);
            }
        } catch (Throwable t) {
            log.errorWsCommandExecutionFailure(requestClassName, session.getId(), endpoint, t);
            String errorMessage = "BusCommand failed [" + requestClassName + "]";
            sendErrorResponse(session, errorMessage, t);
        }
    }

    /**
     * When a message is received from a WebSocket client, this method will lookup the {@link WsCommand} for the
     * given request class and execute it.
     *
     * @param nameAndJsonStr the name of the API request followed by "=" followed then by the request's JSON data
     * @param session the client session making the request
     */
    @OnMessage
    public void onMessage(String nameAndJsonStr, Session session) {
        String requestClassName = "?";
        try {
            // parse the JSON and get its message POJO
            BasicMessageWithExtraData<BasicMessage> request = new ApiDeserializer().deserialize(nameAndJsonStr);
            requestClassName = request.getBasicMessage().getClass().getName();
            log.infoReceivedWsMessage(requestClassName, session.getId(), endpoint);

            handleRequest(session, request);

        } catch (WebsocketAuthenticationException wae) {
            try {
                session.close(new CloseReason(CloseCodes.VIOLATED_POLICY, wae.getLocalizedMessage()));
            } catch (IOException ioe) {
                log.errorCloseSessionAfterAuthFailure(ioe, session.getId(), endpoint, requestClassName);
            }
        } catch (Throwable t) {
            log.errorWsCommandExecutionFailure(requestClassName, session.getId(), endpoint, t);
            String errorMessage = "Failed to process message [" + requestClassName + "]";
            sendErrorResponse(session, errorMessage, t);
        }
    }

    protected void sendErrorResponse(Session session, String errorMessage, Throwable t) {
        BasicMessageWithExtraData<GenericErrorResponse> response = new BasicMessageWithExtraData<>(
                new GenericErrorResponseBuilder().setThrowable(t).setErrorMessage(errorMessage).build(), null);
        try {
            new WebSocketHelper().sendSync(session, response);
        } catch (IOException ioe) {
            log.errorf(ioe, "Failed to send a message [%s] to WebSocket client session [%s] of endpoint [%s]",
                    response.getBasicMessage().getClass().getName(), session.getId(), endpoint);
        }
    }

}