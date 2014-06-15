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

public class Sub extends XSub
{

    public static class SubSession extends XSub.XSubSession
    {

        public SubSession(final IOThread io_thread_, final boolean connect_,
                          final SocketBase socket_, final Options options_,
                          final Address addr_)
        {
            super(io_thread_, connect_, socket_, options_, addr_);
        }

    }

    public Sub(final Ctx parent_, final int tid_, final int sid_)
    {
        super(parent_, tid_, sid_);

        options.type = ZMQ.ZMQ_SUB;

        // Switch filtering messages on (as opposed to XSUB which where the
        // filtering is off).
        options.filter = true;
    }

    @Override
    public boolean xsetsockopt(final int option_, final Object optval_)
    {
        if (option_ != ZMQ.ZMQ_SUBSCRIBE && option_ != ZMQ.ZMQ_UNSUBSCRIBE) {
            return false;
        }

        byte[] val;

        if (optval_ instanceof String) {
            val = ((String) optval_).getBytes(ZMQ.CHARSET);
        }
        else if (optval_ instanceof byte[]) {
            val = (byte[]) optval_;
        }
        else {
            throw new IllegalArgumentException();
        }

        // Create the subscription message.
        final Msg msg = new Msg(val.length + 1);
        if (option_ == ZMQ.ZMQ_SUBSCRIBE) {
            msg.put((byte) 1);
        }
        else if (option_ == ZMQ.ZMQ_UNSUBSCRIBE) {
            msg.put((byte) 0);
        }
        msg.put(val);
        // Pass it further on in the stack.
        final boolean rc = super.xsend(msg);
        if (!rc) {
            throw new IllegalStateException(
                                            "Failed to send subscribe/unsubscribe message");
        }

        return true;
    }

    @Override
    protected boolean xsend(final Msg msg_)
    {
        // Overload the XSUB's send.
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean xhas_out()
    {
        // Overload the XSUB's send.
        return false;
    }

}
