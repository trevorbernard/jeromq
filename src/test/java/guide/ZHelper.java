package guide;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class ZHelper
{
    private static Random rand = new Random(System.currentTimeMillis());

    /**
     * Receives all message parts from socket, prints neatly
     */
    public static void dump(final Socket sock)
    {
        System.out.println("----------------------------------------");
        while (true) {
            final byte[] msg = sock.recv(0);
            boolean isText = true;
            String data = "";
            for (byte element : msg) {
                if (element < 32 || element > 127) {
                    isText = false;
                }
                data += String.format("%02X", element);
            }
            if (isText) {
                data = new String(msg, ZMQ.CHARSET);
            }

            System.out.println(String.format("[%03d] %s", msg.length, data));
            if (!sock.hasReceiveMore()) {
                break;
            }
        }
    }

    public static void setId(final Socket sock)
    {
        final String identity = String.format("%04X-%04X", rand.nextInt(),
                                              rand.nextInt());

        sock.setIdentity(identity.getBytes(ZMQ.CHARSET));
    }

    public static List<Socket> buildZPipe(final Context ctx)
    {
        final Socket socket1 = ctx.socket(ZMQ.PAIR);
        socket1.setLinger(0);
        socket1.setHWM(1);

        final Socket socket2 = ctx.socket(ZMQ.PAIR);
        socket2.setLinger(0);
        socket2.setHWM(1);

        final String iface = "inproc://"
                             + new BigInteger(130, rand).toString(32);
        socket1.bind(iface);
        socket2.connect(iface);

        return Arrays.asList(socket1, socket2);
    }
}
