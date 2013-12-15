/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.sctp.nio;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.AbstractNioChannelOutboundBuffer;
import io.netty.channel.sctp.SctpMessage;

final class NioSctpChannelOutboundBuffer extends AbstractNioChannelOutboundBuffer {
    NioSctpChannelOutboundBuffer(NioSctpChannel channel) {
        super(channel);
    }

    @Override
    protected long addMessage(Object msg, ChannelPromise promise) {
        if (msg instanceof SctpMessage) {
            SctpMessage packet = (SctpMessage) msg;
            ByteBuf content = packet.content();
            if (!content.isDirect() || content.nioBufferCount() > 1) {
                ByteBuf buf = toDirect(content);
                msg = new SctpMessage(packet.protocolIdentifier(), packet.streamIdentifier(), buf);
            }
        }
        return super.addMessage(msg, promise);
    }
}
