package org.zeromq.perf;

import java.util.EnumSet;

import org.zeromq.api.SocketFlags;
import org.zeromq.api.SocketType;
import org.zeromq.api.ZSocket;

public class LocalLatency {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("usage: local_lat <bind-to> <message-size> <roundtrip-count>");
            System.exit(1);
        }
        String bindTo = args[0];
        int messageSize = Integer.valueOf(args[1]);
        int roundtripCount = Integer.valueOf(args[2]);

        ZSocket socket = new ZSocket(SocketType.REP);
        socket.bind(bindTo);

        for (int i = 0; i != roundtripCount; i++) {
            byte[] rc = socket.receive();
            if (rc.length != messageSize) {
                System.out.println("message of incorrect size received");
                System.exit(1);
            }
            socket.send(rc, EnumSet.of(SocketFlags.NONE));
        }
        socket.close();
    }
}
