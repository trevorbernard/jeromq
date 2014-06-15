package guide;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Clone server model 1
 * @author Danish Shrestha <dshrestha06@gmail.com>
 */
public class clonesrv1
{
    private static AtomicLong sequence = new AtomicLong();

    public void run()
    {
        final Context ctx = ZMQ.context(1);
        final Socket publisher = ctx.socket(ZMQ.PUB);
        publisher.bind("tcp://*:5556");

        try {
            Thread.sleep(200);
        }
        catch (final InterruptedException e) {
            e.printStackTrace();
        }

        final Random random = new Random();

        while (true) {
            final long currentSequenceNumber = sequence.incrementAndGet();
            final int key = random.nextInt(10000);
            final int body = random.nextInt(1000000);

            final ByteBuffer b = ByteBuffer.allocate(4);
            b.asIntBuffer().put(body);

            final kvsimple kvMsg = new kvsimple(key + "",
                                                currentSequenceNumber,
                                                b.array());
            kvMsg.send(publisher);
            System.out.println("sending " + kvMsg);

        }
    }

    public static void main(final String[] args)
    {
        new clonesrv1().run();
    }
}
