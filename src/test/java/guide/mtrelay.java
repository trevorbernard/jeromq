package guide;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Multithreaded relay
 */
public class mtrelay
{

    private static class Step1 extends Thread
    {
        private final Context context;

        private Step1(final Context context)
        {
            this.context = context;
        }

        @Override
        public void run()
        {
            // Signal downstream to step 2
            final Socket xmitter = context.socket(ZMQ.PAIR);
            xmitter.connect("inproc://step2");
            System.out.println("Step 1 ready, signaling step 2");
            xmitter.send("READY", 0);
            xmitter.close();
        }

    }

    private static class Step2 extends Thread
    {
        private final Context context;

        private Step2(final Context context)
        {
            this.context = context;
        }

        @Override
        public void run()
        {
            // Bind to inproc: endpoint, then start upstream thread
            final Socket receiver = context.socket(ZMQ.PAIR);
            receiver.bind("inproc://step2");
            final Thread step1 = new Step1(context);
            step1.start();

            // Wait for signal
            receiver.recv(0);
            receiver.close();

            // Connect to step3 and tell it we're ready
            final Socket xmitter = context.socket(ZMQ.PAIR);
            xmitter.connect("inproc://step3");
            xmitter.send("READY", 0);

            xmitter.close();
        }

    }

    public static void main(final String[] args)
    {

        final Context context = ZMQ.context(1);

        // Bind to inproc: endpoint, then start upstream thread
        final Socket receiver = context.socket(ZMQ.PAIR);
        receiver.bind("inproc://step3");

        // Step 2 relays the signal to step 3
        final Thread step2 = new Step2(context);
        step2.start();

        // Wait for signal
        receiver.recv(0);
        receiver.close();

        System.out.println("Test successful!");
        context.term();
    }
}
