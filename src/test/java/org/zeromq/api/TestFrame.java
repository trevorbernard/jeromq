package org.zeromq.api;

import org.junit.Assert;
import org.junit.Test;

public class TestFrame {

    @Test
    public void testSimpleFrame() {
        Context context = null;
        Socket push = null;
        Socket pull = null;
        try {
            context = new Context();
            push = context.createSocket(SocketType.PUSH);
            push.connect("tcp://127.0.0.1:5693");

            pull = context.createSocket(SocketType.PULL);
            pull.bind("tcp://*:5693");
            byte[] expected = new byte[] { 'h', 'e', 'l', 'l', 'o' };
            Frame f = Frame.createFrame(expected, false, false);
            push.sendFrame(f);

            Frame actual = pull.receiveFrame();
            Assert.assertArrayEquals(expected, actual.getData());
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
