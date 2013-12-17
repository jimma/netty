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
import io.netty.channel.ChannelFlushPromiseNotifier.FlushCheckpoint;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.AbstractNioChannelOutboundBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

final class NioSocketChannelOutboundBuffer extends AbstractNioChannelOutboundBuffer {

    private static final int INITIAL_CAPACITY = 32;
    private final Queue<FlushCheckpoint> promises =
            new ArrayDeque<FlushCheckpoint>();
    private final NioSocketChannel channel;

    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;
    private long totalPending;
    private long writeCounter;
    private boolean inNotify;

    NioSocketChannelOutboundBuffer(NioSocketChannel channel) {
        super(channel);
        this.channel = channel;
        nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
    }

    @Override
    protected long addMessage(Object msg, ChannelPromise promise) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;

            // Check if we are inNotify or if the last entry is not the same as the first
            // This is needed because someone may write to the channel in a ChannelFutureListener which then
            // could lead to have the buffer merged into the current buffer. In this case the current buffer may be
            // removed as it was completely written before.
            if (buf.isReadable() && channel.config().isWriteBufferAutoMerge() && (!inNotify || last() != first())) {
                NioEntry entry = last();
                if (entry != null) {
                    int size = entry.merge(buf, promise);

                    if (size != -1) {
                        return size;
                    }
                }
                if (!buf.isDirect()) {
                    msg = toDirect(promise.channel(), buf);
                }
            }
        }
        long size = super.addMessage(msg, promise);
        assert size >= 0;
        totalPending += size;
        addPromise(promise);
        return size;
    }

    private void addPromise(ChannelPromise promise) {
        if (isVoidPromise(promise)) {
            // no need to add the promises to later notify if it is a VoidChannelPromise
            return;
        }
        FlushCheckpoint checkpoint;
        if (promise instanceof FlushCheckpoint) {
            checkpoint = (FlushCheckpoint) promise;
            checkpoint.flushCheckpoint(totalPending);
        } else {
            checkpoint = new DefaultFlushCheckpoint(totalPending, promise);
        }

        promises.offer(checkpoint);
    }

    @Override
    protected void addFlush() {
        super.addFlush();
    }

    @Override
    public void progress(long amount) {
        super.progress(amount);
        if (amount > 0) {
            writeCounter += amount;
            notifyPromises(null);
        }
    }

    @Override
    protected NioEntry first() {
        return (NioEntry) super.first();
    }

    @Override
    protected NioEntry last() {
        return (NioEntry) super.last();
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
            ByteBuffer[] nioBuffers = this.nioBuffers;
            NioEntry entry = first();
            int i = size();

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

        @Override
        public void success() {
            writeCounter += pendingSize();
            safeRelease(msg);
            notifyPromises(null);
            decrementPendingOutboundBytes(pendingSize());
        }

        @Override
        public void fail(Throwable cause, boolean decrementAndNotify) {
            writeCounter += pendingSize();
            safeRelease(msg);
            notifyPromises(cause);

            if (decrementAndNotify) {
                decrementPendingOutboundBytes(pendingSize());
            }
        }

        private int merge(ByteBuf buffer, ChannelPromise promise) {
            if (!(msg instanceof ByteBuf)) {
                return -1;
            }
            ByteBuf last = (ByteBuf) msg;
            int readable = buffer.readableBytes();
            if (last.nioBufferCount() == 1 && last.isWritable(readable)) {
                // reset cached stuff
                buffers = null;
                buf = null;
                count = -1;

                // merge bytes in and release the original buffer
                last.writeBytes(buffer);
                safeRelease(buffer);

                totalPending += readable;
                addPromise(promise);

                pendingSize += readable;
                total += readable;

                // increment pending bytes after adding message to the unflushed arrays.
                // See https://github.com/netty/netty/issues/1619
                incrementPendingOutboundBytes(readable);
                return readable;
            }
            return -1;
        }
    }

    private void notifyPromises(Throwable cause) {
        try {
            inNotify = true;
            final long writeCounter = this.writeCounter;
            for (;;) {
                FlushCheckpoint cp = promises.peek();
                if (cp == null) {
                    // Reset the counter if there's nothing in the notification list.
                    this.writeCounter = 0;
                    totalPending = 0;
                    break;
                }

                if (cp.flushCheckpoint() > writeCounter) {
                    if (writeCounter > 0 && promises.size() == 1) {
                        this.writeCounter = 0;
                        totalPending -= writeCounter;
                        cp.flushCheckpoint(cp.flushCheckpoint() - writeCounter);
                    }
                    break;
                }

                promises.remove();
                if (cause == null) {
                    cp.promise().trySuccess();
                } else {
                    safeFail(cp.promise(), cause);
                }
            }

            // Avoid overflow
            final long newWriteCounter = this.writeCounter;
            if (newWriteCounter >= 0x8000000000L) {
                // Reset the counter only when the counter grew pretty large
                // so that we can reduce the cost of updating all entries in the notification list.
                this.writeCounter = 0;
                totalPending -= newWriteCounter;
                for (FlushCheckpoint cp: promises) {
                    cp.flushCheckpoint(cp.flushCheckpoint() - newWriteCounter);
                }
            }
        } finally {
            inNotify = false;
        }
    }

    private static class DefaultFlushCheckpoint implements FlushCheckpoint {
        private long checkpoint;
        private final ChannelPromise future;

        DefaultFlushCheckpoint(long checkpoint, ChannelPromise future) {
            this.checkpoint = checkpoint;
            this.future = future;
        }

        @Override
        public long flushCheckpoint() {
            return checkpoint;
        }

        @Override
        public void flushCheckpoint(long checkpoint) {
            this.checkpoint = checkpoint;
        }

        @Override
        public ChannelPromise promise() {
            return future;
        }
    }
}
