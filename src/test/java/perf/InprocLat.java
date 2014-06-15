/*
    Copyright (c) 2007-2012 iMatix Corporation
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2012 Other contributors as noted in the AUTHORS file
                
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
package perf;

import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

public class InprocLat
{

    static class Worker implements Runnable
    {

        private final Ctx ctx_;
        private final int roundtrip_count;

        Worker(final Ctx ctx, final int roundtrip_count)
        {
            this.ctx_ = ctx;
            this.roundtrip_count = roundtrip_count;
        }

        @Override
        public void run()
        {
            final SocketBase s = ZMQ.zmq_socket(ctx_, ZMQ.ZMQ_REP);
            if (s == null) {
                printf("error in zmq_socket: %s\n");
                exit(1);
            }

            final boolean rc = ZMQ.zmq_connect(s, "inproc://lat_test");
            if (!rc) {
                printf("error in zmq_connect: %s\n");
                exit(1);
            }

            Msg msg;

            for (int i = 0; i != roundtrip_count; i++) {
                msg = ZMQ.zmq_recvmsg(s, 0);
                if (msg == null) {
                    printf("error in zmq_recvmsg: %s\n");
                    exit(1);
                }
                final int r = ZMQ.zmq_sendmsg(s, msg, 0);
                if (r < 0) {
                    printf("error in zmq_sendmsg: %s\n");
                    exit(1);
                }
            }

            ZMQ.zmq_close(s);
        }

        private void exit(final int i)
        {
            // TODO Auto-generated method stub

        }
    }

    public static void main(final String[] argv) throws Exception
    {
        if (argv.length != 2) {
            printf("usage: inproc_lat <message-size> <roundtrip-count>\n");
            return;
        }

        final int message_size = atoi(argv[0]);
        final int roundtrip_count = atoi(argv[1]);

        final Ctx ctx = ZMQ.zmq_init(1);
        if (ctx == null) {
            printf("error in zmq_init:");
            return;
        }

        final SocketBase s = ZMQ.zmq_socket(ctx, ZMQ.ZMQ_REQ);
        if (s == null) {
            printf("error in zmq_socket: ");
            return;
        }

        final boolean rc = ZMQ.zmq_bind(s, "inproc://lat_test");
        if (!rc) {
            printf("error in zmq_bind: ");
            return;
        }

        final Thread local_thread = new Thread(new Worker(ctx, roundtrip_count));
        local_thread.start();

        final Msg smsg = ZMQ.zmq_msg_init_size(message_size);

        printf("message size: %d [B]\n", message_size);
        printf("roundtrip count: %d\n", roundtrip_count);

        final long watch = ZMQ.zmq_stopwatch_start();

        for (int i = 0; i != roundtrip_count; i++) {
            final int r = ZMQ.zmq_sendmsg(s, smsg, 0);
            if (r < 0) {
                printf("error in zmq_sendmsg: %s\n");
                return;
            }
            final Msg msg = ZMQ.zmq_recvmsg(s, 0);
            if (msg == null) {
                printf("error in zmq_recvmsg: %s\n");
                return;
            }
            if (ZMQ.zmq_msg_size(msg) != message_size) {
                printf("message of incorrect size received\n");
                return;
            }
        }

        final long elapsed = ZMQ.zmq_stopwatch_stop(watch);

        final double latency = (double) elapsed / (roundtrip_count * 2);

        local_thread.join();

        printf("average latency: %.3f [us]\n", latency);

        ZMQ.zmq_close(s);

        ZMQ.zmq_term(ctx);
    }

    private static int atoi(final String string)
    {
        return Integer.parseInt(string);
    }

    private static void printf(final String string)
    {
        System.out.println(string);
    }

    private static void printf(final String string, final Object... args)
    {
        System.out.println(String.format(string, args));
    }

}
