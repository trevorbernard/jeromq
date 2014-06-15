package guide;

//  Suicidal Snail

import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread;
import org.zeromq.ZThread.IAttachedRunnable;

public class suisnail
{
    private static final long MAX_ALLOWED_DELAY = 1000; // msecs
    private static Random rand = new Random(System.currentTimeMillis());

    // This is our subscriber. It connects to the publisher and subscribes
    // to everything. It sleeps for a short time between messages to
    // simulate doing too much work. If a message is more than one second
    // late, it croaks.
    private static class Subscriber implements IAttachedRunnable
    {
        @Override
        public void run(final Object[] args, final ZContext ctx,
                        final Socket pipe)
        {
            // Subscribe to everything
            final Socket subscriber = ctx.createSocket(ZMQ.SUB);
            subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);
            subscriber.connect("tcp://localhost:5556");

            // Get and process messages
            while (true) {
                final String string = subscriber.recvStr();
                System.out.printf("%s\n", string);
                final long clock = Long.parseLong(string);

                // Suicide snail logic
                if (System.currentTimeMillis() - clock > MAX_ALLOWED_DELAY) {
                    System.err.println("E: subscriber cannot keep up, aborting");
                    break;
                }
                // Work for 1 msec plus some random additional time
                try {
                    Thread.sleep(1000 + rand.nextInt(2000));
                }
                catch (final InterruptedException e) {
                    break;
                }
            }
            pipe.send("gone and died");
        }
    }

    // .split publisher task
    // This is our publisher task. It publishes a time-stamped message to its
    // PUB socket every millisecond:
    private static class Publisher implements IAttachedRunnable
    {
        @Override
        public void run(final Object[] args, final ZContext ctx,
                        final Socket pipe)
        {
            // Prepare publisher
            final Socket publisher = ctx.createSocket(ZMQ.PUB);
            publisher.bind("tcp://*:5556");

            while (true) {
                // Send current clock (msecs) to subscribers
                final String string = String.format("%d",
                                                    System.currentTimeMillis());
                publisher.send(string);
                final String signal = pipe.recvStr(ZMQ.DONTWAIT);
                if (signal != null) {
                    break;
                }
                try {
                    Thread.sleep(1);
                }
                catch (final InterruptedException e) {
                }
            }

        }
    }

    // .split main task
    // The main task simply starts a client and a server, and then
    // waits for the client to signal that it has died:
    public static void main(final String[] args) throws Exception
    {
        final ZContext ctx = new ZContext();
        final Socket pubpipe = ZThread.fork(ctx, new Publisher());
        final Socket subpipe = ZThread.fork(ctx, new Subscriber());
        subpipe.recvStr();
        pubpipe.send("break");
        Thread.sleep(100);
        ctx.destroy();
    }
}
