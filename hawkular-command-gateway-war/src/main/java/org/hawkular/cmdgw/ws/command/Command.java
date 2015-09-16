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

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.BasicMessageWithExtraData;
import org.hawkular.bus.common.BinaryData;

/**
 * An command that comes from a feed.
 */
public interface Command<REQ extends BasicMessage, RESP extends BasicMessage> {

    /**
     * Performs the command for the feed.
     *
     * @param request the request that describes what needs to be executed
     * @param binaryData if not null, this contains extra binary data that came across with the command request
     * @param context some context data that can be useful for the command to be able to execute the request
     * @return the results of the command that need to be sent back to the feed - may be null
     * @throws Exception if failed to execute the operation
     */
    BasicMessageWithExtraData<RESP> execute(REQ request, BinaryData binaryData, CommandContext context)
            throws Exception;
}
