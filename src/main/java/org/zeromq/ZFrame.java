/*
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.
                
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
package org.zeromq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import org.zeromq.ZMQ.Socket;

/**
 * ZFrame The ZFrame class provides methods to send and receive single message
 * frames across 0MQ sockets. A 'frame' corresponds to one underlying zmq_msg_t
 * in the libzmq code. When you read a frame from a socket, the more() method
 * indicates if the frame is part of an unfinished multipart message. The send()
 * method normally destroys the frame, but with the ZFRAME_REUSE flag, you can
 * send the same frame many times. Frames are binary, and this class has no
 * special support for text data.
 */

public class ZFrame
{

    public static final int MORE = ZMQ.SNDMORE;
    public static final int REUSE = 128; // no effect at java
    public static final int DONTWAIT = ZMQ.DONTWAIT;

    private boolean more;

    private byte[] data;

    /**
     * Class Constructor Creates an empty frame. (Useful when reading frames
     * from a 0MQ Socket)
     */
    protected ZFrame()
    {
    }

    /**
     * Class Constructor Copies message data into ZFrame object
     * @param data
     *            Data to copy into ZFrame object
     */
    public ZFrame(final byte[] data)
    {
        if (data != null) {
            this.data = data;
        }
    }

    /**
     * Class Constructor Copies String into frame data
     * @param data
     */
    public ZFrame(final String data)
    {
        if (data != null) {
            this.data = data.getBytes(ZMQ.CHARSET);
        }
    }

    /**
     * Destructor.
     */
    public void destroy()
    {
        if (hasData()) {
            data = null;
        }
    }

    /**
     * @return the data
     */
    public byte[] getData()
    {
        return data;
    }

    /**
     * @return More flag, true if last read had MORE message parts to come
     */
    public boolean hasMore()
    {
        return more;
    }

    /**
     * Returns byte size of frame, if set, else 0
     * @return Number of bytes in frame data, else 0
     */
    public int size()
    {
        if (hasData()) {
            return data.length;
        }
        else {
            return 0;
        }
    }

    /**
     * Convenience method to ascertain if this frame contains some message data
     * @return True if frame contains data
     */
    public boolean hasData()
    {
        return data != null;
    }

    /**
     * Internal method to call org.zeromq.Socket send() method.
     * @param socket
     *            0MQ socket to send on
     * @param flags
     *            Valid send() method flags, defined in org.zeromq.ZMQ class
     * @return True if success, else False
     */
    public boolean send(final Socket socket, final int flags)
    {
        if (socket == null) {
            throw new IllegalArgumentException("socket parameter must be set");
        }

        return socket.send(data, flags);
    }

    /**
     * Sends frame to socket if it contains any data. Frame contents are kept
     * after the send.
     * @param socket
     *            0MQ socket to send frame
     * @param flags
     *            Valid send() method flags, defined in org.zeromq.ZMQ class
     * @return True if success, else False
     */
    public boolean sendAndKeep(final Socket socket, final int flags)
    {
        return send(socket, flags);
    }

    /**
     * Sends frame to socket if it contains any data. Frame contents are kept
     * after the send. Uses default behaviour of Socket.send() method, with no
     * flags set
     * @param socket
     *            0MQ socket to send frame
     * @return True if success, else False
     */
    public boolean sendAndKeep(final Socket socket)
    {
        return sendAndKeep(socket, 0);
    }

    /**
     * Sends frame to socket if it contains data. Use this method to send a
     * frame and destroy the data after.
     * @param socket
     *            0MQ socket to send frame
     * @param flags
     *            Valid send() method flags, defined in org.zeromq.ZMQ class
     * @return True if success, else False
     */
    public boolean sendAndDestroy(final Socket socket, final int flags)
    {
        final boolean ret = send(socket, flags);
        if (ret) {
            destroy();
        }
        return ret;
    }

    /**
     * Sends frame to socket if it contains data. Use this method to send an
     * isolated frame and destroy the data after. Uses default behaviour of
     * Socket.send() method, with no flags set
     * @param socket
     *            0MQ socket to send frame
     * @return True if success, else False
     */
    public boolean sendAndDestroy(final Socket socket)
    {
        return sendAndDestroy(socket, 0);
    }

    /**
     * Creates a new frame that duplicates an existing frame
     * @return Duplicate of frame; message contents copied into new byte array
     */
    public ZFrame duplicate()
    {
        return new ZFrame(this.data);
    }

    /**
     * Returns true if both frames have byte - for byte identical data
     * @param other
     *            The other ZFrame to compare
     * @return True if both ZFrames have same byte-identical data, else false
     */
    public boolean hasSameData(final ZFrame other)
    {
        if (other == null) {
            return false;
        }

        if (size() == other.size()) {
            return Arrays.equals(data, other.data);
        }
        return false;
    }

    /**
     * Sets new contents for frame
     * @param data
     *            New byte array contents for frame
     */
    public void reset(final String data)
    {
        this.data = data.getBytes(ZMQ.CHARSET);
    }

    /**
     * Sets new contents for frame
     * @param data
     *            New byte array contents for frame
     */
    public void reset(final byte[] data)
    {
        this.data = data;
    }

    /**
     * @return frame data as a printable hex string
     */
    public String strhex()
    {
        final String hexChar = "0123456789ABCDEF";

        final StringBuilder b = new StringBuilder();
        for (byte element : data) {
            final int b1 = element >>> 4 & 0xf;
            final int b2 = element & 0xf;
            b.append(hexChar.charAt(b1));
            b.append(hexChar.charAt(b2));
        }
        return b.toString();
    }

    /**
     * String equals. Uses String compareTo for the comparison (lexigraphical)
     * @param str
     *            String to compare with frame data
     * @return True if frame body data matches given string
     */
    public boolean streq(final String str)
    {
        if (!hasData()) {
            return false;
        }
        return new String(this.data, ZMQ.CHARSET).compareTo(str) == 0;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ZFrame zFrame = (ZFrame) o;

        if (!Arrays.equals(data, zFrame.data)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }

    /**
     * Returns a human - readable representation of frame's data
     * @return A text string or hex-encoded string if data contains any
     *         non-printable ASCII characters
     */
    @Override
    public String toString()
    {
        if (!hasData()) {
            return "";
        }
        // Dump message as text or hex-encoded string
        boolean isText = true;
        for (byte element : data) {
            if (element < 32 || element > 127) {
                isText = false;
            }
        }
        if (isText) {
            return new String(data, ZMQ.CHARSET);
        }
        else {
            return strhex();
        }
    }

    /**
     * Internal method to call recv on the socket. Does not trap any
     * ZMQExceptions but expects caling routine to handle them.
     * @param socket
     *            0MQ socket to read from
     * @return byte[] data
     */
    private byte[] recv(final Socket socket, final int flags)
    {
        if (socket == null) {
            throw new IllegalArgumentException(
                                               "socket parameter must not be null");
        }

        data = socket.recv(flags);
        more = socket.hasReceiveMore();
        return data;
    }

    /**
     * Receives single frame from socket, returns the received frame object, or
     * null if the recv was interrupted. Does a blocking recv, if you want to
     * not block then use recvFrame(socket, ZMQ.DONTWAIT);
     * @param socket
     *            Socket to read from
     * @return received frame, else null
     */
    public static ZFrame recvFrame(final Socket socket)
    {
        final ZFrame f = new ZFrame();
        f.recv(socket, 0);
        return f;
    }

    /**
     * Receive a new frame off the socket, Returns newly-allocated frame, or
     * null if there was no input waiting, or if the read was interrupted.
     * @param socket
     *            Socket to read from
     * @param flags
     *            Pass flags to 0MQ socket.recv call
     * @return received frame, else null
     */
    public static ZFrame recvFrame(final Socket socket, final int flags)
    {
        final ZFrame f = new ZFrame();
        final byte[] data = f.recv(socket, flags);
        if (data == null) {
            return null;
        }
        return f;
    }

    public void print(final String prefix)
    {

        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);

        if (prefix != null) {
            pw.printf("%s", prefix);
        }
        final byte[] data = getData();
        int size = size();

        boolean is_bin = false;
        int char_nbr;
        for (char_nbr = 0; char_nbr < size; char_nbr++) {
            if (data[char_nbr] < 9 || data[char_nbr] > 127) {
                is_bin = true;
            }
        }

        pw.printf("[%03d] ", size);
        final int max_size = is_bin ? 35 : 70;
        String elipsis = "";
        if (size > max_size) {
            size = max_size;
            elipsis = "...";
        }
        for (char_nbr = 0; char_nbr < size; char_nbr++) {
            if (is_bin) {
                pw.printf("%02X", data[char_nbr]);
            }
            else {
                pw.printf("%c", data[char_nbr]);
            }
        }
        pw.printf("%s\n", elipsis);
        pw.flush();
        pw.close();
        try {
            sw.close();
        }
        catch (final IOException e) {
        }

        System.out.print(sw.toString());
    }

}
