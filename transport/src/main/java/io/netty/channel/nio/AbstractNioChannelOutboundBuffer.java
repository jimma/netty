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
package io.netty.channel.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;

public abstract class AbstractNioChannelOutboundBuffer extends ChannelOutboundBuffer {
    protected AbstractNioChannelOutboundBuffer(AbstractNioChannel channel) {
        super(channel);
    }

    protected static ByteBuf toDirect(Channel channel, ByteBuf buf) {
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
