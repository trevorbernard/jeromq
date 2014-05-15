/*
    Copyright (c) 2007-2012 iMatix Corporation
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2011 Other contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package zmq;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Msg {
    private static final int MAX_VSM_SIZE = 29;

    private static final byte TYPE_MIN = 101;
    private static final byte TYPE_VSM = 101;
    private static final byte TYPE_LMSG = 102;
    private static final byte TYPE_DELIMITER = 103;
    private static final byte TYPE_CMSG = 104;
    private static final byte TYPE_MAX = 104;

    public static final int MORE = 1;
    public static final int COMMAND = 2;
    public static final int IDENTITY = 64;
    public static final int SHARED = 128;

    private int flags;
    private byte type;

    private int capacity;
    private byte[] byteArray;
    private ByteBuffer byteBuffer;

    public Msg() {
        this.type = TYPE_VSM;
        this.byteArray = new byte[0];
        this.byteBuffer = ByteBuffer.wrap(byteArray);
    }

    public Msg(int capacity) {
        if (capacity <= MAX_VSM_SIZE)
            this.type = TYPE_VSM;
        else
            this.type = TYPE_LMSG;
        this.capacity = capacity;
        this.byteArray = new byte[capacity];
        this.byteBuffer = ByteBuffer.wrap(byteArray).order(ByteOrder.BIG_ENDIAN);
    }

    public Msg(byte[] byteArray) {
        if (byteArray == null) {
            byteArray = new byte[0];
        }
        if (byteArray.length <= MAX_VSM_SIZE) {
            this.type = TYPE_VSM;
        } else {
            this.type = TYPE_LMSG;
        }
        this.capacity = byteArray.length;
        this.byteArray = byteArray;
        this.byteBuffer = ByteBuffer.wrap(byteArray).order(ByteOrder.BIG_ENDIAN);
    }

    public Msg(byte[] byteArray, int pos, int lim) {
        int count = Math.min((byteArray.length - pos), lim);
        byte[] buf = new byte[count];
        System.arraycopy(byteArray, pos, buf, 0, count);
        if (buf.length <= MAX_VSM_SIZE) {
            this.type = TYPE_VSM;
        } else {
            this.type = TYPE_LMSG;
        }
        this.capacity = buf.length;
        this.byteArray = buf;
        this.byteBuffer = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
    }

    public Msg(final ByteBuffer src) {
        if (src == null) {
            throw new IllegalArgumentException("ByteBuffer cannot be null");
        }
        if (src.position() > 0) {
            throw new IllegalArgumentException("ByteBuffer position is not zero, did you forget to flip it?");
        }
        this.type = TYPE_LMSG;
        this.flags = 0;
        this.byteBuffer = src.duplicate();
        if (byteBuffer.hasArray())
            this.byteArray = byteBuffer.array();
        else
            this.byteArray = null;
        this.capacity = byteBuffer.remaining();
    }

    public Msg(final Msg msg) {
        if (msg == null) {
            throw new IllegalArgumentException("Msg cannot be null");
        }
        this.type = msg.type;
        this.flags = msg.flags;
        this.capacity = msg.capacity;
        this.byteBuffer = msg.byteBuffer != null ? msg.byteBuffer.duplicate() : null;
        this.byteArray = new byte[this.capacity];
        System.arraycopy(msg.byteArray, 0, this.byteArray, 0, msg.capacity);
    }

    public static Msg createDelimiter() {
        final Msg msg = new Msg();
        msg.type = TYPE_DELIMITER;
        return msg;
    }

    public boolean isIdentity() {
        return (flags & IDENTITY) == IDENTITY;
    }

    public boolean isDelimiter() {
        return type == TYPE_DELIMITER;
    }

    public boolean isVSM() {
        return type == TYPE_VSM;
    }

    public boolean isCMSG() {
        return type == TYPE_CMSG;
    }

    public boolean check() {
        return type >= TYPE_MIN && type <= TYPE_MAX;
    }

    public int flags() {
        return flags;
    }

    public boolean hasMore() {
        return (flags & MORE) > 0;
    }

    public byte type() {
        return type;
    }

    public void setFlags(int flags_) {
        flags |= flags_;
    }

    public void initDelimiter() {
        type = TYPE_DELIMITER;
        flags = 0;
    }

    public byte[] data() {
        if (byteBuffer.isDirect()) {
            int length = byteBuffer.remaining();
            byte[] bytes = new byte[length];
            byteBuffer.duplicate().get(bytes);
            return bytes;
        }
        return byteArray;
    }

    public ByteBuffer buf() {
        final ByteBuffer duplicate;
        if (null == byteBuffer) {
            if (null != byteArray) {
                duplicate = ByteBuffer.wrap(byteArray);
            } else {
                return ByteBuffer.allocate(0);
            }
        } else {
            duplicate = byteBuffer.duplicate();
        }
        duplicate.clear();
        return duplicate;
    }

    public int size() {
        return capacity;
    }

    public void resetFlags(int f) {
        flags = flags & ~f;
    }

    public Msg put(byte b) {
        byteBuffer.put(b);
        return this;
    }

    public Msg put(byte[] src) {
        return put(src, 0, src.length);
    }

    public Msg put(byte[] src, int off, int len) {
        if (src == null)
            return this;
        byteBuffer.put(src, off, len);
        return this;
    }

    public int getBytes(int index, byte[] dst, int off, int len) {
        int count = Math.min(len, capacity);
        if (byteBuffer.isDirect()) {
            ByteBuffer dup = byteBuffer.duplicate();
            dup.position(index);
            dup.put(dst, off, count);
            return count;
        }
        System.arraycopy(byteArray, index, dst, off, count);
        return count;
    }

    @Override
    public String toString() {
        return String.format("#zmq.Msg{type=%s, size=%s, flags=%s}", type, capacity, flags);
    }
}
