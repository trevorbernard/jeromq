package org.zeromq;

import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class ZMQQueue implements Runnable
{

    private final Socket inSocket;
    private final Socket outSocket;

    /**
     * Class constructor.
     * @param context
     *            a 0MQ context previously created.
     * @param inSocket
     *            input socket
     * @param outSocket
     *            output socket
     */
    public ZMQQueue(final Context context, final Socket inSocket,
                    final Socket outSocket)
    {
        this.inSocket = inSocket;
        this.outSocket = outSocket;
    }

    @Override
    public void run()
    {
        zmq.ZMQ.zmq_proxy(inSocket.base(), outSocket.base(), null);
    }

}
