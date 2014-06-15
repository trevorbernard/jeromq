/*
    Copyright (c) 2007-2012 iMatix Corporation
    Copyright (c) 2011 250bpm s.r.o.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class TestReqrepInproc
{
    // Create REQ/ROUTER wiring.

    @Test
    public void testReqrepInproc()
    {
        final Ctx ctx = ZMQ.zmq_init(1);
        assertThat(ctx, notNullValue());
        final SocketBase sb = ZMQ.zmq_socket(ctx, ZMQ.ZMQ_REP);
        assertThat(sb, notNullValue());
        boolean brc = ZMQ.zmq_bind(sb, "inproc://a");
        assertThat(brc, is(true));

        final SocketBase sc = ZMQ.zmq_socket(ctx, ZMQ.ZMQ_REQ);
        assertThat(sc, notNullValue());
        brc = ZMQ.zmq_connect(sc, "inproc://a");
        assertThat(brc, is(true));

        Helper.bounce(sb, sc);

        // Tear down the wiring.
        ZMQ.zmq_close(sb);
        ZMQ.zmq_close(sc);
        ZMQ.zmq_term(ctx);
    }
}
