package guide;

import org.zeromq.ZMQ;

/**
 * Task worker - design 2 Adds pub-sub flow to receive and respond to kill
 * signal
 */
public class taskwork2
{

    public static void main(final String[] args) throws InterruptedException
    {
        final ZMQ.Context context = ZMQ.context(1);

        final ZMQ.Socket receiver = context.socket(ZMQ.PULL);
        receiver.connect("tcp://localhost:5557");

        final ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.connect("tcp://localhost:5558");

        final ZMQ.Socket controller = context.socket(ZMQ.SUB);
        controller.connect("tcp://localhost:5559");
        controller.subscribe(ZMQ.SUBSCRIPTION_ALL);

        final ZMQ.Poller items = new ZMQ.Poller(2);
        items.register(receiver, ZMQ.Poller.POLLIN);
        items.register(controller, ZMQ.Poller.POLLIN);

        while (true) {

            items.poll();

            if (items.pollin(0)) {

                final String message = receiver.recvStr(0);
                final long nsec = Long.parseLong(message);

                // Simple progress indicator for the viewer
                System.out.print(message + '.');
                System.out.flush();

                // Do the work
                Thread.sleep(nsec);

                // Send results to sink
                sender.send("", 0);
            }
            // Any waiting controller command acts as 'KILL'
            if (items.pollin(1)) {
                break; // Exit loop
            }

        }

        // Finished
        receiver.close();
        sender.close();
        controller.close();
        context.term();
    }
}
