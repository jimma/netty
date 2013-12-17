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
/*
 * Written by Josh Bloch of Google Inc. and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * (Transport implementors only) an internal data structure used by {@link AbstractChannel} to store its pending
 * outbound write requests.
 */
public class ChannelOutboundBuffer {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);
    protected AbstractChannel channel;

    private Entry first;
    private Entry last;
    private int flushed;
    private int messages;
    private boolean inFail;

    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");

    private volatile long totalPendingSize;

    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> WRITABLE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "writable");
    private volatile int writable = 1;

    protected ChannelOutboundBuffer() {
        // only for sub-classes
    }

    public ChannelOutboundBuffer(AbstractChannel channel) {
        this.channel = channel;
    }

    protected Entry first() {
        return first;
    }

    protected Entry last() {
        return last;
    }

    protected long addMessage(Object msg, ChannelPromise promise) {
        int size = channel.estimatorHandle().size(msg);
        if (size < 0) {
            size = 0;
        }

        Entry e = newEntry();
        if (last == null) {
            first = e;
            last = e;
        } else {
            last.next = e;
            e.prev = last;
            last = e;
        }
        e.msg = msg;
        e.pendingSize = size;
        e.promise = promise;
        e.total = total(msg);

        messages++;

        // increment pending bytes after adding message to the unflushed arrays.
        // See https://github.com/netty/netty/issues/1619
        incrementPendingOutboundBytes(size);
        return size;
    }

    protected void addFlush() {
        flushed = messages;
    }

    /**
     * Increment the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    protected void incrementPendingOutboundBytes(int size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue + size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue + size;
        }

        int highWaterMark = channel.config().getWriteBufferHighWaterMark();

        if (newWriteBufferSize > highWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 1, 0)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    protected void decrementPendingOutboundBytes(long size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue - size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue - size;
        }

        int lowWaterMark = channel.config().getWriteBufferLowWaterMark();

        if (newWriteBufferSize == 0 || newWriteBufferSize < lowWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 0, 1)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    protected static long total(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    public Object current() {
        if (isEmpty()) {
            return null;
        } else {
            return first.msg;
        }
    }

    public void progress(long amount) {
        Entry e = first;
        e.pendingSize -= amount;
        ChannelPromise p = e.promise;
        if (p instanceof ChannelProgressivePromise) {
            long progress = e.progress + amount;
            e.progress = progress;
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
        }
    }

    protected static boolean isVoidPromise(ChannelPromise promise) {
        return promise instanceof VoidChannelPromise;
    }

    public boolean remove() {
        if (isEmpty()) {
            return false;
        }

        Entry e = first;
        first = e.next;
        if (first == null) {
            last = null;
        }

        messages--;
        flushed--;
        e.success();

        return true;
    }

    public boolean remove(Throwable cause) {
        if (isEmpty()) {
            return false;
        }

        Entry e = first;
        first = e.next;
        if (first == null) {
            last = null;
        }

        messages--;
        flushed--;
        e.fail(cause, true);

        return true;
    }

    boolean getWritable() {
        return writable != 0;
    }

    public int size() {
        return flushed;
    }

    public boolean isEmpty() {
        return flushed == 0;
    }

    void failFlushed(Throwable cause) {
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {
            return;
        }

        try {
            inFail = true;
            for (;;) {
                if (!remove(cause)) {
                    break;
                }
            }
        } finally {
            inFail = false;
        }
    }

    void close(final ClosedChannelException cause) {
        if (inFail) {
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause);
                }
            });
            return;
        }

        inFail = true;

        if (channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        final int unflushedCount = messages - flushed;

        try {
            for (int i = 0; i < unflushedCount; i++) {
                Entry e = last;
                e.fail(cause, false);

                // Just decrease; do not trigger any events via decrementPendingOutboundBytes()
                long size = e.pendingSize;
                long oldValue = totalPendingSize;
                long newWriteBufferSize = oldValue - size;
                while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
                    oldValue = totalPendingSize;
                    newWriteBufferSize = oldValue - size;
                }

                last = e.prev;
            }
        } finally {
            messages = flushed;
            inFail = false;
        }
        recycle();
    }

    protected static void safeRelease(Object message) {
        try {
            ReferenceCountUtil.release(message);
        } catch (Throwable t) {
            logger.warn("Failed to release a message.", t);
        }
    }

    protected static void safeFail(ChannelPromise promise, Throwable cause) {
        if (!isVoidPromise(promise) && !promise.tryFailure(cause)) {
            logger.warn("Promise done already: {} - new exception is:", promise, cause);
        }
    }

    protected Entry newEntry() {
        return Entry.newInstance(this);
    }

    protected static class Entry {
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
            @Override
            protected Entry newObject(Handle handle) {
                return new Entry(handle);
            }
        };

        public static Entry newInstance(ChannelOutboundBuffer buffer) {
            Entry entry = RECYCLER.get();
            entry.buffer = buffer;
            return entry;
        }

        protected final Recycler.Handle entryHandle;
        private Object msg;
        private ChannelPromise promise;
        private long progress;
        protected long total;
        protected long pendingSize;
        protected Entry next;
        private Entry prev;
        protected ChannelOutboundBuffer buffer;

        protected Entry(Recycler.Handle entryHandle) {
            this.entryHandle = entryHandle;
        }

        public Entry next() {
            return next;
        }

        public Entry prev() {
            return prev;
        }

        public Object msg() {
            return msg;
        }

        public void success() {
            try {
                safeRelease(msg);
                promise.trySuccess();
                buffer.decrementPendingOutboundBytes(pendingSize);
            } finally {
                clear();
            }
        }

        public void fail(Throwable cause, boolean decrementAndNotify) {
            try {
                safeRelease(msg);
                safeFail(promise, cause);
                if (decrementAndNotify) {
                    buffer.decrementPendingOutboundBytes(pendingSize);
                }
            } finally {
                clear();
            }
        }

        public long pendingSize() {
            return pendingSize;
        }

        protected void clear() {
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            next = null;
            prev = null;
            buffer = null;
            recycle();
        }

        protected void recycle() {
            RECYCLER.recycle(this, entryHandle);
        }
    }

    protected void recycle() {
        channel = null;
        first = null;
        last = null;
        flushed = 0;
        messages = 0;
        totalPendingSize = 0;
        writable = 1;
    }
}
