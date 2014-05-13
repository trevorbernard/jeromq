package org.zeromq.api;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.zeromq.api.SocketType;
import com.zeromq.api.ZSocket;

public class ZSocketTest {
    @Test
    public void asdf() {
        ZSocket push = null;
        ZSocket pull = null;
        try {
            push = new ZSocket(SocketType.PUSH);
            pull = new ZSocket(SocketType.PULL);
            pull.bind("tcp://*:1337");
            push.connect("tcp://localhost:1337");

            byte[] b = "hello,world!".getBytes();
            push.send(b, 6, 5, 0);

            byte[] actual = pull.receive(0);
            System.out.println("**" + new String(actual));
            assertEquals("world", new String(actual));

        } finally {
            try {
                push.close();
            } catch (Exception e) {
            }
            try {
                pull.close();
            } catch (Exception e) {
            }
        }
    }
}
