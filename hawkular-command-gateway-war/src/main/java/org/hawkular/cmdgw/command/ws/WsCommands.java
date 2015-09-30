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
package org.hawkular.cmdgw.command.ws;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.cmdgw.NoCommandForMessageException;
import org.hawkular.cmdgw.api.EchoRequest;
import org.hawkular.cmdgw.api.ResourcePathDestination;
import org.hawkular.cmdgw.api.UiSessionDestination;

/**
 * A registry of {@link WsCommand} that can operate on messages coming over a WebSocket.
 *
 * @author <a href="https://github.com/ppalaga">Peter Palaga</a>
 */
@ApplicationScoped
public class WsCommands {

    private final ResourcePathDestinationWsCommand resourcePathDestinationWsCommand = //
    new ResourcePathDestinationWsCommand();

    private final EchoCommand echoCommand = new EchoCommand();

    private final UiSessionDestinationWsCommand uiSessionDestinationWsCommand = new UiSessionDestinationWsCommand();

    /**
     * Returns a {@link WsCommand} that should handle the given {@code requestClass}.
     *
     * @param requestClass the type of a request for which a processing {@link WsCommand} should be found by this method
     * @return a {@link WsCommand}, never {@code null}
     * @throws NoCommandForMessageException if no {@link WsCommand} was found
     */
    @SuppressWarnings("unchecked")
    public <REQ extends BasicMessage> WsCommand<REQ> getCommand(Class<REQ> requestClass)
            throws NoCommandForMessageException {
        if (ResourcePathDestination.class.isAssignableFrom(requestClass)) {
            return (WsCommand<REQ>) resourcePathDestinationWsCommand;
        } else if (UiSessionDestination.class.isAssignableFrom(requestClass)) {
            return (WsCommand<REQ>) uiSessionDestinationWsCommand;
        } else if (EchoRequest.class.isAssignableFrom(requestClass)) {
            return (WsCommand<REQ>) echoCommand;
        }
        // new commands will most probably have to be else-iffed here
        throw new NoCommandForMessageException("No command found for requestClass [" + requestClass.getName() + "]");
    }
}
