package guide;

import java.util.Random;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

//  Pathological subscriber
//  Subscribes to one random topic and prints received messages
public class pathosub
{
    public static void main(final String[] args)
    {
        final ZContext context = new ZContext();
        final Socket subscriber = context.createSocket(ZMQ.SUB);
        if (args.length == 1) {
            subscriber.connect(args[0]);
        }
        else {
            subscriber.connect("tcp://localhost:5556");
        }

        final Random rand = new Random(System.currentTimeMillis());
        final String subscription = String.format("%03d", rand.nextInt(1000));
        subscriber.subscribe(subscription.getBytes(ZMQ.CHARSET));

        while (true) {
            final String topic = subscriber.recvStr();
            if (topic == null) {
                break;
            }
            final String data = subscriber.recvStr();
            assert (topic.equals(subscription));
            System.out.println(data);
        }
        context.destroy();
    }
}
