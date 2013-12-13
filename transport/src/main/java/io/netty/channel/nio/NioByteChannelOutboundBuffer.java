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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.util.Recycler;
import io.netty.util.internal.SystemPropertyUtil;

import java.nio.ByteBuffer;

public final class NioByteChannelOutboundBuffer extends ChannelOutboundBuffer {

private static final int INITIAL_CAPACITY = 32;
    private static final int threadLocalDirectBufferSize;

    static {
        threadLocalDirectBufferSize = SystemPropertyUtil.getInt("io.netty.threadLocalDirectBufferSize", 64 * 1024);
        logger.debug("-Dio.netty.threadLocalDirectBufferSize: {}", threadLocalDirectBufferSize);
    }

    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;

    NioByteChannelOutboundBuffer(AbstractChannel channel) {
        super(channel);
        nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
    }

    @Override
    protected void addMessage(Object msg, ChannelPromise promise) {
        super.addMessage(msg, promise);
    }

    @Override
    protected void addFlush() {
        super.addFlush();
    }

    @Override
    public Object current() {
        if (isEmpty()) {
            return null;
        } else {
            Object msg = super.current();
            if (threadLocalDirectBufferSize <= 0) {
                return msg;
            }
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                if (buf.isDirect()) {
                    return buf;
                } else {
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
                    current(directBuf);
                    return directBuf;
                }
            }
            return msg;
        }
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@code null} is returned otherwise.  If this method returns a non-null array, {@link #nioBufferCount()} and
     * {@link #nioBufferSize()} will return the number of NIO buffers in the returned array and the total number
     * of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link io.netty.channel.AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link io.netty.channel.socket.nio.NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     */
    public ByteBuffer[] nioBuffers() {
        long nioBufferSize = 0;
        int nioBufferCount = 0;

        if (!isEmpty()) {
            final ByteBufAllocator alloc = channel.alloc();
            ByteBuffer[] nioBuffers = this.nioBuffers;
            NioEntry entry = (NioEntry) first;
            int i = flushed;

            for (;;) {
                Object m = entry.msg();
                if (!(m instanceof ByteBuf)) {
                    this.nioBufferCount = 0;
                    this.nioBufferSize = 0;
                    return null;
                }

                ByteBuf buf = (ByteBuf) m;
                final int readerIndex = buf.readerIndex();
                final int readableBytes = buf.writerIndex() - readerIndex;

                if (readableBytes > 0) {
                    nioBufferSize += readableBytes;
                    int count = entry.count;
                    if (count == -1) {
                        entry.count = count = buf.nioBufferCount();
                    }
                    int neededSpace = nioBufferCount + count;
                    if (neededSpace > nioBuffers.length) {
                        this.nioBuffers = nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                    }

                    if (buf.isDirect() || threadLocalDirectBufferSize <= 0) {
                        if (count == 1) {
                            ByteBuffer nioBuf = entry.buf;
                            if (nioBuf == null) {
                                // cache ByteBuffer as it may need to create a new ByteBuffer instance if its a
                                // derived buffer
                                entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                            }
                            nioBuffers[nioBufferCount ++] = nioBuf;
                        } else {
                            ByteBuffer[] nioBufs = entry.buffers;
                            if (nioBufs == null) {
                                // cached ByteBuffers as they may be expensive to create in terms of Object allocation
                                entry.buffers = nioBufs = buf.nioBuffers();
                            }
                            nioBufferCount = fillBufferArray(nioBufs, nioBuffers, nioBufferCount);
                        }
                    } else {
                        nioBufferCount = fillBufferArrayNonDirect(entry, buf, readerIndex,
                                readableBytes, alloc, nioBuffers, nioBufferCount);
                    }
                }
                if (--i == 0) {
                    break;
                }
                entry = entry.next();
            }
        }

        this.nioBufferCount = nioBufferCount;
        this.nioBufferSize = nioBufferSize;

        return nioBuffers;
    }

    private static int fillBufferArray(ByteBuffer[] nioBufs, ByteBuffer[] nioBuffers, int nioBufferCount) {
        for (ByteBuffer nioBuf: nioBufs) {
            if (nioBuf == null) {
                break;
            }
            nioBuffers[nioBufferCount ++] = nioBuf;
        }
        return nioBufferCount;
    }

    private static int fillBufferArrayNonDirect(NioEntry entry, ByteBuf buf, int readerIndex, int readableBytes,
                                                ByteBufAllocator alloc, ByteBuffer[] nioBuffers, int nioBufferCount) {
        ByteBuf directBuf;
        if (alloc.isDirectBufferPooled()) {
            directBuf = alloc.directBuffer(readableBytes);
        } else {
            directBuf = ThreadLocalPooledByteBuf.newInstance();
        }
        directBuf.writeBytes(buf, readerIndex, readableBytes);
        buf.release();
        entry.replaceMessage(directBuf);
        // cache ByteBuffer
        ByteBuffer nioBuf = entry.buf = directBuf.internalNioBuffer(0, readableBytes);
        entry.count = 1;
        nioBuffers[nioBufferCount ++] = nioBuf;
        return nioBufferCount;
    }

    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        ByteBuffer[] newArray = new ByteBuffer[newCapacity];
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    public int nioBufferCount() {
        return nioBufferCount;
    }

    public long nioBufferSize() {
        return nioBufferSize;
    }

    @Override
    protected NioEntry newEntry() {
        return new NioEntry();
    }

    final class NioEntry extends Entry {
        ByteBuffer[] buffers;
        ByteBuffer buf;
        int count = -1;

        @Override
        public NioEntry next() {
            return (NioEntry) super.next();
        }

        @Override
        public NioEntry prev() {
            return (NioEntry) super.prev();
        }

        protected void replaceMessage(Object msg) {
            this.msg = msg;
        }
    }

    static final class ThreadLocalPooledByteBuf extends UnpooledDirectByteBuf {
        private final Recycler.Handle handle;

        private static final Recycler<ThreadLocalPooledByteBuf> RECYCLER = new Recycler<ThreadLocalPooledByteBuf>() {
            @Override
            protected ThreadLocalPooledByteBuf newObject(Handle handle) {
                return new ThreadLocalPooledByteBuf(handle);
            }
        };

        private ThreadLocalPooledByteBuf(Recycler.Handle handle) {
            super(UnpooledByteBufAllocator.DEFAULT, 256, Integer.MAX_VALUE);
            this.handle = handle;
        }

        static ThreadLocalPooledByteBuf newInstance() {
            ThreadLocalPooledByteBuf buf = RECYCLER.get();
            buf.setRefCnt(1);
            return buf;
        }

        @Override
        protected void deallocate() {
            if (capacity() > threadLocalDirectBufferSize) {
                super.deallocate();
            } else {
                clear();
                RECYCLER.recycle(this, handle);
            }
        }
    }
}
