package guide;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

//  Freelance server - Model 2
//  Does some work, replies OK, with message sequencing
public class flserver2
{
    public static void main(final String[] args)
    {
        if (args.length < 1) {
            System.out.printf("I: syntax: flserver2 <endpoint>\n");
            System.exit(0);
        }
        final ZContext ctx = new ZContext();
        final Socket server = ctx.createSocket(ZMQ.REP);
        server.bind(args[0]);

        System.out.printf("I: echo service is ready at %s\n", args[0]);
        while (true) {
            final ZMsg request = ZMsg.recvMsg(server);
            if (request == null) {
                break; // Interrupted
            }

            // Fail nastily if run against wrong client
            assert (request.size() == 2);

            final ZFrame identity = request.pop();
            request.destroy();

            final ZMsg reply = new ZMsg();
            reply.add(identity);
            reply.add("OK");
            reply.send(server);
        }
        if (Thread.currentThread().isInterrupted()) {
            System.out.printf("W: interrupted\n");
        }

        ctx.destroy();
    }
}
