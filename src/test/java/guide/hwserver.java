package guide;

//
//  Hello World server in Java
//  Binds REP socket to tcp://*:5555
//  Expects "Hello" from client, replies with "World"
//

import org.zeromq.ZMQ;

public class hwserver
{

    public static void main(final String[] args) throws Exception
    {
        final ZMQ.Context context = ZMQ.context(1);

        // Socket to talk to clients
        final ZMQ.Socket socket = context.socket(ZMQ.REP);

        socket.bind("tcp://*:5555");

        while (!Thread.currentThread().isInterrupted()) {

            final byte[] reply = socket.recv(0);
            System.out.println("Received " + ": ["
                               + new String(reply, ZMQ.CHARSET) + "]");

            // Create a "Hello" message.
            final String request = "world";
            // Send the message
            socket.send(request.getBytes(ZMQ.CHARSET), 0);

            Thread.sleep(1000); // Do some 'work'
        }

        socket.close();
        context.term();
    }
}
