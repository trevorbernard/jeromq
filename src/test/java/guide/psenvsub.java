package guide;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Pubsub envelope subscriber
 */

public class psenvsub
{

    public static void main(final String[] args)
    {

        // Prepare our context and subscriber
        final Context context = ZMQ.context(1);
        final Socket subscriber = context.socket(ZMQ.SUB);

        subscriber.connect("tcp://localhost:5563");
        subscriber.subscribe("B".getBytes(ZMQ.CHARSET));
        while (!Thread.currentThread().isInterrupted()) {
            // Read envelope with address
            final String address = subscriber.recvStr();
            // Read message contents
            final String contents = subscriber.recvStr();
            System.out.println(address + " : " + contents);
        }
        subscriber.close();
        context.term();
    }
}
