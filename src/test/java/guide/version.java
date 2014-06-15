package guide;

import org.zeromq.ZMQ;

//  Report 0MQ version
public class version
{

    public static void main(final String[] args)
    {
        System.out.println(String.format("Version string: %s, Version int: %d",
                                         ZMQ.getVersionString(),
                                         ZMQ.getFullVersion()));
    }

}
