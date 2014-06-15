package org.zeromq;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;

import org.junit.Test;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class TestZMQ
{
    static class Client extends Thread
    {

        private Socket s = null;

        public Client(final Context ctx)
        {
            s = ctx.socket(ZMQ.PULL);
        }

        @Override
        public void run()
        {
            System.out.println("Start client thread ");
            s.connect("tcp://127.0.0.1:6669");
            s.recv(0);

            s.close();
            System.out.println("Stop client thread ");
        }
    }

    @Test
    public void testPollerPollout() throws Exception
    {
        final ZMQ.Context context = ZMQ.context(1);
        final Client client = new Client(context);

        // Socket to send messages to
        final ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.bind("tcp://127.0.0.1:6669");

        ZMQ.Poller outItems;
        outItems = context.poller();
        outItems.register(sender, ZMQ.Poller.POLLOUT);

        while (!Thread.currentThread().isInterrupted()) {

            outItems.poll(1000);
            if (outItems.pollout(0)) {
                sender.send("OK", 0);
                System.out.println("ok");
                break;
            }
            else {
                System.out.println("not writable");
                client.start();
            }
        }
        client.join();
        sender.close();
        context.term();
    }

    @Test
    public void testByteBufferSend() throws InterruptedException
    {
        final ZMQ.Context context = ZMQ.context(1);
        final ByteBuffer bb = ByteBuffer.allocate(4)
                                        .order(ByteOrder.nativeOrder());
        ZMQ.Socket push = null;
        ZMQ.Socket pull = null;
        try {
            push = context.socket(ZMQ.PUSH);
            pull = context.socket(ZMQ.PULL);
            pull.bind("tcp://*:12344");
            push.connect("tcp://localhost:12344");
            bb.put("PING".getBytes(ZMQ.CHARSET));
            bb.flip();
            push.sendByteBuffer(bb, 0);
            final String actual = new String(pull.recv(), ZMQ.CHARSET);
            assertEquals("PING", actual);
        }
        finally {
            try {
                push.close();
            }
            catch (final Exception ignore) {
            }
            try {
                pull.close();
            }
            catch (final Exception ignore) {
            }
            try {
                context.term();
            }
            catch (final Exception ignore) {
            }
        }

    }

    @Test
    public void testByteBufferRecv() throws InterruptedException,
                                    CharacterCodingException
    {
        final ZMQ.Context context = ZMQ.context(1);
        final ByteBuffer bb = ByteBuffer.allocate(6)
                                        .order(ByteOrder.nativeOrder());
        ZMQ.Socket push = null;
        ZMQ.Socket pull = null;
        try {
            push = context.socket(ZMQ.PUSH);
            pull = context.socket(ZMQ.PULL);
            pull.bind("tcp://*:12345");
            push.connect("tcp://localhost:12345");
            push.send("PING".getBytes(ZMQ.CHARSET), 0);
            pull.recvByteBuffer(bb, 0);
            bb.flip();
            final byte[] b = new byte[bb.remaining()];
            bb.duplicate().get(b);
            assertEquals("PING", new String(b, ZMQ.CHARSET));
        }
        finally {
            try {
                push.close();
            }
            catch (final Exception ignore) {
            }
            try {
                pull.close();
            }
            catch (final Exception ignore) {
            }
            try {
                context.term();
            }
            catch (final Exception ignore) {
            }
        }

    }

    @Test(expected = ZMQException.class)
    public void testBindSameAddress()
    {
        final ZMQ.Context context = ZMQ.context(1);

        final ZMQ.Socket socket1 = context.socket(ZMQ.REQ);
        final ZMQ.Socket socket2 = context.socket(ZMQ.REQ);
        socket1.bind("tcp://*:12346");
        try {
            socket2.bind("tcp://*:12346");
            fail("Exception not thrown");
        }
        catch (final ZMQException e) {
            assertEquals(e.getErrorCode(), ZMQ.Error.EADDRINUSE.getCode());
            throw e;
        }
        finally {
            socket1.close();
            socket2.close();

            context.term();
        }
    }

    @Test
    public void testEventConnected()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket helper = context.socket(ZMQ.REQ);
        final int port = helper.bindToRandomPort("tcp://127.0.0.1");

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_CONNECTED));
        monitor.connect("inproc://monitor.socket");

        socket.connect("tcp://127.0.0.1:" + port);
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_CONNECTED, event.getEvent());

        helper.close();
        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventConnectDelayed()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_CONNECT_DELAYED));
        monitor.connect("inproc://monitor.socket");

        socket.connect("tcp://127.0.0.1:6751");
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_CONNECT_DELAYED, event.getEvent());

        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventConnectRetried()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_CONNECT_RETRIED));
        monitor.connect("inproc://monitor.socket");

        socket.connect("tcp://127.0.0.1:6752");
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_CONNECT_RETRIED, event.getEvent());

        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventListening()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_LISTENING));
        monitor.connect("inproc://monitor.socket");

        socket.bindToRandomPort("tcp://127.0.0.1");
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_LISTENING, event.getEvent());

        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventBindFailed()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket helper = context.socket(ZMQ.REP);
        final int port = helper.bindToRandomPort("tcp://127.0.0.1");

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_BIND_FAILED));
        monitor.connect("inproc://monitor.socket");

        try {
            socket.bind("tcp://127.0.0.1:" + port);
        }
        catch (final ZMQException ex) {
        }
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_BIND_FAILED, event.getEvent());

        helper.close();
        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventAccepted()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        final Socket helper = context.socket(ZMQ.REQ);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket", ZMQ.EVENT_ACCEPTED));
        monitor.connect("inproc://monitor.socket");

        final int port = socket.bindToRandomPort("tcp://127.0.0.1");

        helper.connect("tcp://127.0.0.1:" + port);
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_ACCEPTED, event.getEvent());

        helper.close();
        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventClosed()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        socket.bindToRandomPort("tcp://127.0.0.1");

        assertTrue(socket.monitor("inproc://monitor.socket", ZMQ.EVENT_CLOSED));
        monitor.connect("inproc://monitor.socket");

        socket.close();
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_CLOSED, event.getEvent());

        monitor.close();
        context.term();
    }

    @Test
    public void testEventDisconnected()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        final Socket helper = context.socket(ZMQ.REQ);
        monitor.setReceiveTimeOut(100);

        final int port = socket.bindToRandomPort("tcp://127.0.0.1");
        helper.connect("tcp://127.0.0.1:" + port);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_DISCONNECTED));
        monitor.connect("inproc://monitor.socket");

        helper.close();
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_DISCONNECTED, event.getEvent());

        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testEventMonitorStopped()
    {
        final Context context = ZMQ.context(1);
        ZMQ.Event event;

        final Socket socket = context.socket(ZMQ.REP);
        final Socket monitor = context.socket(ZMQ.PAIR);
        monitor.setReceiveTimeOut(100);

        assertTrue(socket.monitor("inproc://monitor.socket",
                                  ZMQ.EVENT_MONITOR_STOPPED));
        monitor.connect("inproc://monitor.socket");

        socket.monitor(null, 0);
        event = ZMQ.Event.recv(monitor);
        assertNotNull("No event was received", event);
        assertEquals(ZMQ.EVENT_MONITOR_STOPPED, event.getEvent());

        socket.close();
        monitor.close();
        context.term();
    }

    @Test
    public void testSocketUnbind()
    {
        final Context context = ZMQ.context(1);

        final Socket push = context.socket(ZMQ.PUSH);
        final Socket pull = context.socket(ZMQ.PULL);
        pull.setReceiveTimeOut(50);

        final int port = pull.bindToRandomPort("tcp://127.0.0.1");
        push.connect("tcp://127.0.0.1:" + port);

        final byte[] data = "ABC".getBytes();

        push.send(data);
        assertArrayEquals(data, pull.recv());

        pull.unbind("tcp://127.0.0.1:" + port);

        push.send(data);
        assertNull(pull.recv());

        push.close();
        pull.close();
        context.term();
    }
}
