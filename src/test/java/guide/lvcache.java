package guide;

import java.util.HashMap;
import java.util.Map;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;

//  Last value cache
//  Uses XPUB subscription messages to re-send data
public class lvcache
{
    public static void main(final String[] args)
    {
        final ZContext context = new ZContext();
        final Socket frontend = context.createSocket(ZMQ.SUB);
        frontend.bind("tcp://*:5557");
        final Socket backend = context.createSocket(ZMQ.XPUB);
        backend.bind("tcp://*:5558");

        // Subscribe to every single topic from publisher
        frontend.subscribe(ZMQ.SUBSCRIPTION_ALL);

        // Store last instance of each topic in a cache
        final Map<String, String> cache = new HashMap<String, String>();

        // .split main poll loop
        // We route topic updates from frontend to backend, and
        // we handle subscriptions by sending whatever we cached,
        // if anything:
        while (true) {
            final PollItem[] items = {
                                      new PollItem(frontend, ZMQ.Poller.POLLIN),
                                      new PollItem(backend, ZMQ.Poller.POLLIN), };
            if (ZMQ.poll(items, 1000) == -1) {
                break; // Interrupted
            }

            // Any new topic data we cache and then forward
            if (items[0].isReadable()) {
                final String topic = frontend.recvStr();
                final String current = frontend.recvStr();

                if (topic == null) {
                    break;
                }
                cache.put(topic, current);
                backend.sendMore(topic);
                backend.send(current);
            }
            // .split handle subscriptions
            // When we get a new subscription, we pull data from the cache:
            if (items[1].isReadable()) {
                final ZFrame frame = ZFrame.recvFrame(backend);
                if (frame == null) {
                    break;
                }
                // Event is one byte 0=unsub or 1=sub, followed by topic
                final byte[] event = frame.getData();
                if (event[0] == 1) {
                    final String topic = new String(event, 1, event.length - 1,
                                                    ZMQ.CHARSET);
                    System.out.printf("Sending cached topic %s\n", topic);
                    final String previous = cache.get(topic);
                    if (previous != null) {
                        backend.sendMore(topic);
                        backend.send(previous);
                    }
                }
                frame.destroy();
            }
        }
        context.destroy();
    }
}
