package guide;

import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZThread;
import org.zeromq.ZThread.IAttachedRunnable;

//  Espresso Pattern
//  This shows how to capture data using a pub-sub proxy
public class espresso
{
    // The subscriber thread requests messages starting with
    // A and B, then reads and counts incoming messages.
    private static class Subscriber implements IAttachedRunnable
    {

        @Override
        public void run(final Object[] args, final ZContext ctx,
                        final Socket pipe)
        {
            // Subscribe to "A" and "B"
            final Socket subscriber = ctx.createSocket(ZMQ.SUB);
            subscriber.connect("tcp://localhost:6001");
            subscriber.subscribe("A".getBytes(ZMQ.CHARSET));
            subscriber.subscribe("B".getBytes(ZMQ.CHARSET));

            int count = 0;
            while (count < 5) {
                final String string = subscriber.recvStr();
                if (string == null) {
                    break; // Interrupted
                }
                count++;
            }
            ctx.destroySocket(subscriber);
        }
    }

    // .split publisher thread
    // The publisher sends random messages starting with A-J:
    private static class Publisher implements IAttachedRunnable
    {
        @Override
        public void run(final Object[] args, final ZContext ctx,
                        final Socket pipe)
        {
            final Socket publisher = ctx.createSocket(ZMQ.PUB);
            publisher.bind("tcp://*:6000");
            final Random rand = new Random(System.currentTimeMillis());

            while (!Thread.currentThread().isInterrupted()) {
                final String string = String.format("%c-%05d",
                                                    'A' + rand.nextInt(10),
                                                    rand.nextInt(100000));
                if (!publisher.send(string)) {
                    break; // Interrupted
                }
                try {
                    Thread.sleep(100); // Wait for 1/10th second
                }
                catch (final InterruptedException e) {
                }
            }
            ctx.destroySocket(publisher);
        }
    }

    // .split listener thread
    // The listener receives all messages flowing through the proxy, on its
    // pipe. In CZMQ, the pipe is a pair of ZMQ_PAIR sockets that connect
    // attached child threads. In other languages your mileage may vary:
    private static class Listener implements IAttachedRunnable
    {
        @Override
        public void run(final Object[] args, final ZContext ctx,
                        final Socket pipe)
        {
            // Print everything that arrives on pipe
            while (true) {
                final ZFrame frame = ZFrame.recvFrame(pipe);
                if (frame == null) {
                    break; // Interrupted
                }
                frame.print(null);
                frame.destroy();
            }
        }
    }

    // .split main thread
    // The main task starts the subscriber and publisher, and then sets
    // itself up as a listening proxy. The listener runs as a child thread:
    public static void main(final String[] argv)
    {
        // Start child threads
        final ZContext ctx = new ZContext();
        ZThread.fork(ctx, new Publisher());
        ZThread.fork(ctx, new Subscriber());

        final Socket subscriber = ctx.createSocket(ZMQ.XSUB);
        subscriber.connect("tcp://localhost:6000");
        final Socket publisher = ctx.createSocket(ZMQ.XPUB);
        publisher.bind("tcp://*:6001");
        final Socket listener = ZThread.fork(ctx, new Listener());
        ZMQ.proxy(subscriber, publisher, listener);

        System.out.println(" interrupted");
        // Tell attached threads to exit
        ctx.destroy();
    }
}
