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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.zeromq.ZMQ.Socket;

/**
 * The ZMsg class provides methods to send and receive multipart messages across
 * 0MQ sockets. This class provides a list-like container interface, with
 * methods to work with the overall container. ZMsg messages are composed of
 * zero or more ZFrame objects.
 * 
 * <pre>
 * // Send a simple single-frame string message on a ZMQSocket &quot;output&quot; socket
 * // object
 * ZMsg.newStringMsg(&quot;Hello&quot;).send(output);
 * 
 * // Add several frames into one message
 * ZMsg msg = new ZMsg();
 * for (int i = 0; i &lt; 10; i++) {
 *     msg.addString(&quot;Frame&quot; + i);
 * }
 * msg.send(output);
 * 
 * // Receive message from ZMQSocket &quot;input&quot; socket object and iterate over frames
 * ZMsg receivedMessage = ZMsg.recvMsg(input);
 * for (ZFrame f : receivedMessage) {
 *     // Do something with frame f (of type ZFrame)
 * }
 * </pre>
 * 
 * Based on <a
 * href="http://github.com/zeromq/czmq/blob/master/src/zmsg.c">zmsg.c</a> in
 * czmq
 */

public class ZMsg implements Iterable<ZFrame>, Deque<ZFrame>
{
    /**
     * Hold internal list of ZFrame objects
     */
    private ArrayDeque<ZFrame> frames;

    /**
     * Class Constructor
     */
    public ZMsg()
    {
        frames = new ArrayDeque<ZFrame>();
    }

    /**
     * Destructor. Explicitly destroys all ZFrames contains in the ZMsg
     */
    public void destroy()
    {
        if (frames == null) {
            return;
        }
        for (final ZFrame f : frames) {
            f.destroy();
        }
        frames.clear();
        frames = null;
    }

    /**
     * @return total number of bytes contained in all ZFrames in this ZMsg
     */
    public long contentSize()
    {
        long size = 0;
        for (final ZFrame f : frames) {
            size += f.size();
        }
        return size;
    }

    /**
     * Add a String as a new ZFrame to the end of list
     * @param str
     *            String to add to list
     */
    public void addString(final String str)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        frames.add(new ZFrame(str));
    }

    /**
     * Creates copy of this ZMsg. Also duplicates all frame content.
     * @return The duplicated ZMsg object, else null if this ZMsg contains an
     *         empty frame set
     */
    public ZMsg duplicate()
    {
        if (frames != null) {
            final ZMsg msg = new ZMsg();
            for (final ZFrame f : frames) {
                msg.add(f.duplicate());
            }
            return msg;
        }
        else {
            return null;
        }
    }

    /**
     * Push frame plus empty frame to front of message, before 1st frame.
     * Message takes ownership of frame, will destroy it when message is sent.
     * @param frame
     */
    public void wrap(final ZFrame frame)
    {
        if (frame != null) {
            push(new ZFrame(""));
            push(frame);
        }
    }

    /**
     * Pop frame off front of message, caller now owns frame. If next frame is
     * empty, pops and destroys that empty frame (e.g. useful when unwrapping
     * ROUTER socket envelopes)
     * @return Unwrapped frame
     */
    public ZFrame unwrap()
    {
        if (size() == 0) {
            return null;
        }
        final ZFrame f = pop();
        ZFrame empty = getFirst();
        if (empty.hasData() && empty.size() == 0) {
            empty = pop();
            empty.destroy();
        }
        return f;
    }

    /**
     * Send message to 0MQ socket.
     * @param socket
     *            0MQ socket to send ZMsg on.
     * @return true if send is success, false otherwise
     */
    public boolean send(final Socket socket)
    {
        return send(socket, true);
    }

    /**
     * Send message to 0MQ socket, destroys contents after sending if destroy
     * param is set to true. If the message has no frames, sends nothing but
     * still destroy()s the ZMsg object
     * @param socket
     *            0MQ socket to send ZMsg on.
     * @return true if send is success, false otherwise
     */
    public boolean send(final Socket socket, final boolean destroy)
    {
        if (socket == null) {
            throw new IllegalArgumentException("socket is null");
        }

        if (frames == null) {
            throw new IllegalArgumentException("destroyed message");
        }

        if (frames.size() == 0) {
            return true;
        }

        boolean ret = true;
        final Iterator<ZFrame> i = frames.iterator();
        while (i.hasNext()) {
            final ZFrame f = i.next();
            ret = f.sendAndKeep(socket, (i.hasNext()) ? ZFrame.MORE : 0);
        }
        if (destroy) {
            destroy();
        }
        return ret;
    }

    /**
     * Receives message from socket, returns ZMsg object or null if the recv was
     * interrupted. Does a blocking recv, if you want not to block then use the
     * ZLoop class or ZMQ.Poller to check for socket input before receiving or
     * recvMsg with flag ZMQ.DONTWAIT.
     * @param socket
     * @return ZMsg object, null if interrupted
     */
    public static ZMsg recvMsg(final Socket socket)
    {
        return recvMsg(socket, 0);
    }

    /**
     * Receives message from socket, returns ZMsg object or null if the recv was
     * interrupted. Setting the flag to ZMQ.DONTWAIT does a non-blocking recv.
     * @param socket
     * @param flag
     *            see ZMQ constants
     * @return ZMsg object, null if interrupted
     */
    public static ZMsg recvMsg(final Socket socket, final int flag)
    {
        if (socket == null) {
            throw new IllegalArgumentException("socket is null");
        }

        ZMsg msg = new ZMsg();

        while (true) {
            final ZFrame f = ZFrame.recvFrame(socket, flag);
            if (f == null) {
                // If receive failed or was interrupted
                msg.destroy();
                msg = null;
                break;
            }
            msg.add(f);
            if (!f.hasMore()) {
                break;
            }
        }
        return msg;
    }

    /**
     * Save message to an open data output stream. Data saved as: 4 bytes:
     * number of frames For every frame: 4 bytes: byte size of frame data + n
     * bytes: frame byte data
     * @param msg
     *            ZMsg to save
     * @param file
     *            DataOutputStream
     * @return True if saved OK, else false
     */
    public static boolean save(final ZMsg msg, final DataOutputStream file)
    {
        if (msg == null) {
            return false;
        }

        try {
            // Write number of frames
            file.writeInt(msg.size());
            if (msg.size() > 0) {
                for (final ZFrame f : msg) {
                    // Write byte size of frame
                    file.writeInt(f.size());
                    // Write frame byte data
                    file.write(f.getData());
                }
            }
            return true;
        }
        catch (final IOException e) {
            return false;
        }
    }

    /**
     * Load / append a ZMsg from an open DataInputStream
     * @param file
     *            DataInputStream connected to file
     * @return ZMsg object
     */
    public static ZMsg load(final DataInputStream file)
    {
        if (file == null) {
            return null;
        }
        final ZMsg rcvMsg = new ZMsg();

        try {
            final int msgSize = file.readInt();
            if (msgSize > 0) {
                int msgNbr = 0;
                while (++msgNbr <= msgSize) {
                    final int frameSize = file.readInt();
                    final byte[] data = new byte[frameSize];
                    file.read(data);
                    rcvMsg.add(new ZFrame(data));
                }
            }
            return rcvMsg;
        }
        catch (final IOException e) {
            return null;
        }
    }

    /**
     * Create a new ZMsg from one or more Strings
     * @param strings
     *            Strings to add as frames.
     * @return ZMsg object
     */
    public static ZMsg newStringMsg(final String... strings)
    {
        final ZMsg msg = new ZMsg();
        for (final String data : strings) {
            msg.addString(data);
        }
        return msg;
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
        final ZMsg zMsg = (ZMsg) o;

        if (frames == null || zMsg.frames == null) {
            return false;
        }

        // based on AbstractList
        final Iterator<ZFrame> e1 = frames.iterator();
        final Iterator<ZFrame> e2 = zMsg.frames.iterator();
        while (e1.hasNext() && e2.hasNext()) {
            final ZFrame o1 = e1.next();
            final ZFrame o2 = e2.next();
            if (!(o1 == null ? o2 == null : o1.equals(o2))) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    @Override
    public int hashCode()
    {
        if (frames == null || frames.size() == 0) {
            return 0;
        }

        int result = 1;
        for (final ZFrame frame : frames) {
            result = 31 * result + (frame == null ? 0 : frame.hashCode());
        }

        return result;
    }

    /**
     * Dump the message in human readable format. This should only be used for
     * debugging and tracing, inefficient in handling large messages.
     **/
    public void dump(final Appendable out)
    {
        try {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            pw.printf("--------------------------------------\n");
            for (final ZFrame frame : frames) {
                pw.printf("[%03d] %s\n", frame.size(), frame.toString());
            }
            out.append(sw.getBuffer());
            sw.close();
        }
        catch (final IOException e) {
            throw new RuntimeException("Message dump exception "
                                       + super.toString(), e);
        }
    }

    public void dump()
    {
        dump(System.out);
    }

    // ********* Convenience Deque methods for common data types *** //

    public void addFirst(final String stringValue)
    {
        addFirst(new ZFrame(stringValue));
    }

    public void addFirst(final byte[] data)
    {
        addFirst(new ZFrame(data));
    }

    public void addLast(final String stringValue)
    {
        addLast(new ZFrame(stringValue));
    }

    public void addLast(final byte[] data)
    {
        addLast(new ZFrame(data));
    }

    // ********* Convenience Queue methods for common data types *** //

    public void push(final String str)
    {
        push(new ZFrame(str));
    }

    public void push(final byte[] data)
    {
        push(new ZFrame(data));
    }

    public boolean add(final String stringValue)
    {
        return add(new ZFrame(stringValue));
    }

    public boolean add(final byte[] data)
    {
        return add(new ZFrame(data));
    }

    // ********* Implement Iterable Interface *************** //
    @Override
    public Iterator<ZFrame> iterator()
    {
        // TODO Auto-generated method stub
        return frames.iterator();
    }

    // ********* Implement Deque Interface ****************** //
    @Override
    public boolean addAll(final Collection<? extends ZFrame> arg0)
    {
        return frames.addAll(arg0);
    }

    @Override
    public void clear()
    {
        frames.clear();

    }

    @Override
    public boolean containsAll(final Collection<?> arg0)
    {
        return frames.containsAll(arg0);
    }

    @Override
    public boolean isEmpty()
    {
        return frames.isEmpty();
    }

    @Override
    public boolean removeAll(final Collection<?> arg0)
    {
        return frames.removeAll(arg0);
    }

    @Override
    public boolean retainAll(final Collection<?> arg0)
    {
        return frames.retainAll(arg0);
    }

    @Override
    public Object[] toArray()
    {
        return frames.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] arg0)
    {
        return frames.toArray(arg0);
    }

    @Override
    public boolean add(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        return frames.add(e);
    }

    @Override
    public void addFirst(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        frames.addFirst(e);
    }

    @Override
    public void addLast(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        frames.addLast(e);

    }

    @Override
    public boolean contains(final Object o)
    {
        return frames.contains(o);
    }

    @Override
    public Iterator<ZFrame> descendingIterator()
    {
        return frames.descendingIterator();
    }

    @Override
    public ZFrame element()
    {
        return frames.element();
    }

    @Override
    public ZFrame getFirst()
    {
        try {
            return frames.getFirst();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public ZFrame getLast()
    {
        try {
            return frames.getLast();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public boolean offer(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        return frames.offer(e);
    }

    @Override
    public boolean offerFirst(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        return frames.offerFirst(e);
    }

    @Override
    public boolean offerLast(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        return frames.offerLast(e);
    }

    @Override
    public ZFrame peek()
    {
        return frames.peek();
    }

    @Override
    public ZFrame peekFirst()
    {
        try {
            return frames.peekFirst();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public ZFrame peekLast()
    {
        try {
            return frames.peekLast();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public ZFrame poll()
    {
        return frames.poll();
    }

    @Override
    public ZFrame pollFirst()
    {
        return frames.pollFirst();
    }

    @Override
    public ZFrame pollLast()
    {
        return frames.pollLast();
    }

    @Override
    public ZFrame pop()
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        try {
            return frames.pop();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Pop a ZFrame and return the toString() representation of it.
     * @return toString version of pop'ed frame, or null if no frame exists.
     */
    public String popString()
    {
        final ZFrame frame = pop();
        if (frame == null) {
            return null;
        }

        return frame.toString();
    }

    @Override
    public void push(final ZFrame e)
    {
        if (frames == null) {
            frames = new ArrayDeque<ZFrame>();
        }
        frames.push(e);
    }

    @Override
    public ZFrame remove()
    {
        return frames.remove();
    }

    @Override
    public boolean remove(final Object o)
    {
        return frames.remove(o);
    }

    @Override
    public ZFrame removeFirst()
    {
        try {
            return frames.removeFirst();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public boolean removeFirstOccurrence(final Object o)
    {
        return frames.removeFirstOccurrence(o);
    }

    @Override
    public ZFrame removeLast()
    {
        try {
            return frames.removeLast();
        }
        catch (final NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public boolean removeLastOccurrence(final Object o)
    {
        return frames.removeLastOccurrence(o);
    }

    @Override
    public int size()
    {
        return frames.size();
    }

    /**
     * Returns pretty string representation of multipart message: [ frame0,
     * frame1, ..., frameN ]
     * @return toString version of ZMsg object
     */
    @Override
    public String toString()
    {
        final StringBuilder out = new StringBuilder("[ ");
        final Iterator<ZFrame> frameIterator = frames.iterator();
        while (frameIterator.hasNext()) {
            out.append(frameIterator.next());
            if (frameIterator.hasNext()) {
                out.append(", "); // skip last iteration
            }
        }
        out.append(" ]");
        return out.toString();
    }

}
