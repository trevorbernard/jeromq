package org.zeromq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class TestProxy
{
    static class Client extends Thread
    {

        private Socket s = null;
        private String name = null;

        public Client(final Context ctx, final String name_)
        {
            s = ctx.socket(ZMQ.REQ);
            name = name_;

            s.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {
            s.connect("tcp://127.0.0.1:6660");
            s.send("hello", 0);
            s.recvStr(0);
            s.send("world", 0);
            s.recvStr(0);

            s.close();
        }
    }

    static class Dealer extends Thread
    {

        private Socket s = null;
        private String name = null;

        public Dealer(final Context ctx, final String name_)
        {
            s = ctx.socket(ZMQ.DEALER);
            name = name_;

            s.setIdentity(name.getBytes(ZMQ.CHARSET));
        }

        @Override
        public void run()
        {

            System.out.println("Start dealer " + name);

            s.connect("tcp://127.0.0.1:6661");
            int count = 0;
            while (count < 2) {
                String msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                final String identity = msg;
                System.out.println(name + " received client identity "
                                   + identity);
                msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                System.out.println(name + " received bottom " + msg);

                msg = s.recvStr(0);
                if (msg == null) {
                    throw new RuntimeException();
                }
                final String data = msg;

                System.out.println(name + " received data " + msg + " " + data);
                s.send(identity, ZMQ.SNDMORE);
                s.send((byte[]) null, ZMQ.SNDMORE);

                final String response = "OK " + data;

                s.send(response, 0);
                count++;
            }
            s.close();
            System.out.println("Stop dealer " + name);
        }
    }

    static class Main extends Thread
    {

        Context ctx;

        Main(final Context ctx_)
        {
            ctx = ctx_;
        }

        @Override
        public void run()
        {
            int port;
            final Socket frontend = ctx.socket(ZMQ.ROUTER);

            assertNotNull(frontend);
            port = frontend.bind("tcp://127.0.0.1:6660");
            assertEquals(port, 6660);

            final Socket backend = ctx.socket(ZMQ.DEALER);
            assertNotNull(backend);
            port = backend.bind("tcp://127.0.0.1:6661");
            assertEquals(port, 6661);

            ZMQ.proxy(frontend, backend, null);

            frontend.close();
            backend.close();
        }

    }

    @Test
    public void testProxy() throws Exception
    {
        final Context ctx = ZMQ.context(1);
        assert (ctx != null);

        final Main mt = new Main(ctx);
        mt.start();
        new Dealer(ctx, "AA").start();
        new Dealer(ctx, "BB").start();

        Thread.sleep(1000);
        final Thread c1 = new Client(ctx, "X");
        c1.start();

        final Thread c2 = new Client(ctx, "Y");
        c2.start();

        c1.join();
        c2.join();

        ctx.term();
    }
}
