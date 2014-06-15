package guide;

import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

//  Broker peering simulation (part 1)
//  Prototypes the state flow

public class peering1
{

    public static void main(final String[] argv)
    {
        // First argument is this broker's name
        // Other arguments are our peers' names
        //
        if (argv.length < 1) {
            System.out.println("syntax: peering1 me {you}\n");
            System.exit(-1);
        }
        final String self = argv[0];
        System.out.println(String.format("I: preparing broker at %s\n", self));
        final Random rand = new Random(System.nanoTime());

        final ZContext ctx = new ZContext();

        // Bind state backend to endpoint
        final Socket statebe = ctx.createSocket(ZMQ.PUB);
        statebe.bind(String.format("ipc://%s-state.ipc", self));

        // Connect statefe to all peers
        final Socket statefe = ctx.createSocket(ZMQ.SUB);
        statefe.subscribe(ZMQ.SUBSCRIPTION_ALL);
        int argn;
        for (argn = 1; argn < argv.length; argn++) {
            final String peer = argv[argn];
            System.out.printf("I: connecting to state backend at '%s'\n", peer);
            statefe.connect(String.format("ipc://%s-state.ipc", peer));
        }
        // The main loop sends out status messages to peers, and collects
        // status messages back from peers. The zmq_poll timeout defines
        // our own heartbeat:

        while (true) {
            // Poll for activity, or 1 second timeout
            final PollItem items[] = { new PollItem(statefe, Poller.POLLIN) };
            final int rc = ZMQ.poll(items, 1000);
            if (rc == -1) {
                break; // Interrupted
            }

            // Handle incoming status messages
            if (items[0].isReadable()) {
                final String peer_name = new String(statefe.recv(0),
                                                    ZMQ.CHARSET);
                final String available = new String(statefe.recv(0),
                                                    ZMQ.CHARSET);
                System.out.printf("%s - %s workers free\n", peer_name,
                                  available);
            }
            else {
                // Send random values for worker availability
                statebe.send(self, ZMQ.SNDMORE);
                statebe.send(String.format("%d", rand.nextInt(10)), 0);
            }
        }
        ctx.destroy();
    }
}
