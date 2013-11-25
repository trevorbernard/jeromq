package org.zeromq.api;

import org.junit.Assert;
import org.junit.Test;

public class TestMessage {

    @Test
    public void testSimpleMessage() {
        Context context = null;
        Socket push = null;
        Socket pull = null;
        try {
            context = new Context();
            push = context.createSocket(SocketType.PUSH);
            push.connect("tcp://127.0.0.1:5693");

            pull = context.createSocket(SocketType.PULL);
            pull.bind("tcp://*:5693");

            Message msg = Message.createMessage(
                    new byte[] { 0 },
                    new byte[] { 'h', 'e', 'l', 'l', 'o' },
                    new byte[] { 'w', 'o', 'r', 'l', 'd' });

            push.sendMessage(msg);
            Message actual = pull.receiveMessage();

            Assert.assertArrayEquals(new byte[] { 0 }, actual.poll().getData());
            Assert.assertArrayEquals(new byte[] { 'h', 'e', 'l', 'l', 'o' }, actual.poll().getData());
            Assert.assertArrayEquals(new byte[] { 'w', 'o', 'r', 'l', 'd' }, actual.poll().getData());
        } finally {
            try {
                push.close();
            } catch (Exception e) {
            }
            try {
                pull.close();
            } catch (Exception e) {
            }
            try {
                context.close();
            } catch (Exception e) {
            }
        }
    }
}
