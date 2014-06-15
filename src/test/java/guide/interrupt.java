package guide;

/*
 *
 *  Interrupt in Java
 *  Shows how to handle Ctrl-C
 */

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class interrupt
{
    public static void main(final String[] args)
    {
        // Prepare our context and socket
        final ZMQ.Context context = ZMQ.context(1);

        final Thread zmqThread = new Thread()
        {
            @Override
            public void run()
            {
                final ZMQ.Socket socket = context.socket(ZMQ.REP);
                socket.bind("tcp://*:5555");

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        socket.recv(0);
                    }
                    catch (final ZMQException e) {
                        if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                            break;
                        }
                    }
                }

                socket.close();
            }
        };

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                System.out.println("W: interrupt received, killing server...");
                context.term();
                try {
                    zmqThread.interrupt();
                    zmqThread.join();
                }
                catch (final InterruptedException e) {
                }
            }
        });

        zmqThread.start();
    }
}
