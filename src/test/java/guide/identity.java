package guide;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Demonstrate identities as used by the request-reply pattern.
 */
public class identity
{

    public static void main(final String[] args) throws InterruptedException
    {

        final Context context = ZMQ.context(1);
        final Socket sink = context.socket(ZMQ.ROUTER);
        sink.bind("inproc://example");

        // First allow 0MQ to set the identity, [00] + random 4byte
        final Socket anonymous = context.socket(ZMQ.REQ);

        anonymous.connect("inproc://example");
        anonymous.send("ROUTER uses a generated UUID", 0);
        ZHelper.dump(sink);

        // Then set the identity ourself
        final Socket identified = context.socket(ZMQ.REQ);
        identified.setIdentity("Hello".getBytes(ZMQ.CHARSET));
        identified.connect("inproc://example");
        identified.send("ROUTER socket uses REQ's socket identity", 0);
        ZHelper.dump(sink);

        sink.close();
        anonymous.close();
        identified.close();
        context.term();

    }
}
