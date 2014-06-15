package guide;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

//  Freelance server - Model 3
//  Uses an ROUTER/ROUTER socket but just one thread
public class flserver3
{
    public static void main(final String[] args)
    {
        final boolean verbose = (args.length > 0 && args[0].equals("-v"));

        final ZContext ctx = new ZContext();
        // Prepare server socket with predictable identity
        final String bindEndpoint = "tcp://*:5555";
        final String connectEndpoint = "tcp://localhost:5555";
        final Socket server = ctx.createSocket(ZMQ.ROUTER);
        server.setIdentity(connectEndpoint.getBytes(ZMQ.CHARSET));
        server.bind(bindEndpoint);
        System.out.printf("I: service is ready at %s\n", bindEndpoint);

        while (!Thread.currentThread().isInterrupted()) {
            final ZMsg request = ZMsg.recvMsg(server);
            if (verbose && request != null) {
                request.dump(System.out);
            }

            if (request == null) {
                break; // Interrupted
            }

            // Frame 0: identity of client
            // Frame 1: PING, or client control frame
            // Frame 2: request body
            final ZFrame identity = request.pop();
            final ZFrame control = request.pop();
            final ZMsg reply = new ZMsg();
            if (control.equals("PING")) {
                reply.add("PONG");
            }
            else {
                reply.add(control);
                reply.add("OK");
            }
            request.destroy();
            reply.push(identity);
            if (verbose && reply != null) {
                reply.dump(System.out);
            }
            reply.send(server);
        }
        if (Thread.currentThread().isInterrupted()) {
            System.out.printf("W: interrupted\n");
        }

        ctx.destroy();
    }
}
