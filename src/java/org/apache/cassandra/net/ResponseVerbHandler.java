/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tracing.Tracing;

public class ResponseVerbHandler implements IVerbHandler  // 处理响应消息，执行回调接口等操作
{
    private static final Logger logger = LoggerFactory.getLogger( ResponseVerbHandler.class );

    public void doVerb(MessageIn message, int id)
    {
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - MessagingService.instance().getRegisteredCallbackAge(id));
        CallbackInfo callbackInfo = MessagingService.instance().removeRegisteredCallback(id);// 取出相应的回调实例
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, id, message.from);
            Tracing.trace(msg, id, message.from);
            return;
        }

        Tracing.trace("Processing response from {}", message.from);
        IAsyncCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            ((IAsyncCallbackWithFailure) cb).onFailure(message.from, message.getFailureReason());
        }
        else
        {
            //TODO: Should we add latency only in success cases?
            MessagingService.instance().maybeAddLatency(cb, message.from, latency);
            cb.response(message);
        }

        if (callbackInfo.callback.supportsBackPressure())
        {
            MessagingService.instance().updateBackPressureOnReceive(message.from, cb, false);
        }
    }
}
