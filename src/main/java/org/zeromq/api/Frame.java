package org.zeromq.api;

import java.util.Arrays;

public class Frame {
    private final byte[] data;
    private final boolean hasMore;

    private Frame(byte[] data, boolean hasMore) {
        this.data = data;
        this.hasMore = hasMore;
    }

    public static Frame createFrame(byte[] data, boolean hasMore, boolean performCopy) {
        if (performCopy) {
            byte[] newData = new byte[data.length];
            System.arraycopy(data, 0, newData, 0, data.length);
            return new Frame(newData, hasMore);
        } else {
            return new Frame(data, hasMore);
        }
    }

    public byte[] getData() {
        return data;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public long size() {
        return (data != null) ? data.length : 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data) + (hasMore ? 1231 : 1237);
    }
}
