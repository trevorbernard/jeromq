package guide;

import java.util.StringTokenizer;

import org.zeromq.ZMQ;

//
//  Weather update client in Java
//  Connects SUB socket to tcp://localhost:5556
//  Collects weather updates and finds avg temp in zipcode
//
public class wuclient
{

    public static void main(final String[] args)
    {
        final ZMQ.Context context = ZMQ.context(1);

        // Socket to talk to server
        System.out.println("Collecting updates from weather server");
        final ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
        subscriber.connect("tcp://localhost:5556");

        // Subscribe to zipcode, default is NYC, 10001
        final String filter = (args.length > 0) ? args[0] : "10001 ";
        subscriber.subscribe(filter.getBytes(ZMQ.CHARSET));

        // Process 100 updates
        int update_nbr;
        long total_temp = 0;
        for (update_nbr = 0; update_nbr < 100; update_nbr++) {
            // Use trim to remove the tailing '0' character
            final String string = subscriber.recvStr(0).trim();

            final StringTokenizer sscanf = new StringTokenizer(string, " ");
            Integer.valueOf(sscanf.nextToken());
            final int temperature = Integer.valueOf(sscanf.nextToken());
            Integer.valueOf(sscanf.nextToken());

            total_temp += temperature;

        }
        System.out.println("Average temperature for zipcode '" + filter
                           + "' was " + (int) (total_temp / update_nbr));

        subscriber.close();
        context.term();
    }
}
