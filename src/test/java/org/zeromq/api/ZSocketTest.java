package org.zeromq.api;

import static org.junit.Assert.assertEquals;

import java.util.EnumSet;

import org.junit.Test;

import com.zeromq.api.SocketFlags;
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
            push.send("world".getBytes(), EnumSet.noneOf(SocketFlags.class));
            byte[] actual = pull.receive(0);
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

    // @Test
    public void asdfasdf() {
        ZSocket push = null;
        ZSocket pull = null;
        try {
            push = new ZSocket(SocketType.PUSH);
            pull = new ZSocket(SocketType.PULL);
            pull.bind("tcp://*:1337");
            push.connect("tcp://localhost:1337");

            byte[] b = "hello".getBytes();
            push.send(b, EnumSet.of(SocketFlags.SEND_MORE));

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
