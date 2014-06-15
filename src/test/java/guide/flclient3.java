package guide;

import org.zeromq.ZMsg;

//  Freelance client - Model 3
//  Uses flcliapi class to encapsulate Freelance pattern
public class flclient3
{
    public static void main(final String[] argv)
    {
        // Create new freelance client object
        final flcliapi client = new flcliapi();

        // Connect to several endpoints
        client.connect("tcp://localhost:5555");
        client.connect("tcp://localhost:5556");
        client.connect("tcp://localhost:5557");

        // Send a bunch of name resolution 'requests', measure time
        int requests = 10000;
        final long start = System.currentTimeMillis();
        while (requests-- > 0) {
            final ZMsg request = new ZMsg();
            request.add("random name");
            final ZMsg reply = client.request(request);
            if (reply == null) {
                System.out.printf("E: name service not available, aborting\n");
                break;
            }
            reply.destroy();
        }
        System.out.printf("Average round trip cost: %d usec\n",
                          (int) (System.currentTimeMillis() - start) / 10);

        client.destroy();
    }

}
