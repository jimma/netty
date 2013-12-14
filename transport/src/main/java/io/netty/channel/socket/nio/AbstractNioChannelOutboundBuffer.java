/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.socket.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;

abstract class AbstractNioChannelOutboundBuffer extends ChannelOutboundBuffer {
    protected AbstractNioChannelOutboundBuffer(AbstractChannel channel) {
        super(channel);
    }

    @Override
    protected long addMessage(Object msg, ChannelPromise promise) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (!buf.isDirect()) {
                msg = toDirect(buf);
            }
        }
        return super.addMessage(msg, promise);
    }

    protected final ByteBuf toDirect(ByteBuf buf) {
        int readableBytes = buf.readableBytes();
        if (readableBytes == 0) {
            return buf;
        }

        // Non-direct buffers are copied into JDK's own internal direct buffer on every I/O.
        // We can do a better job by using our pooled allocator. If the current allocator does not
        // pool a direct buffer, we use a ThreadLocal based pool.
        ByteBufAllocator alloc = channel.alloc();
        ByteBuf directBuf;
        if (alloc.isDirectBufferPooled()) {
            directBuf = alloc.directBuffer(readableBytes);
        } else {
            directBuf = ThreadLocalPooledByteBuf.newInstance();
        }
        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
        safeRelease(buf);
        return directBuf;
    }
}
