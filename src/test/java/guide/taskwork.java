package guide;

import org.zeromq.ZMQ;

//
//  Task worker in Java
//  Connects PULL socket to tcp://localhost:5557
//  Collects workloads from ventilator via that socket
//  Connects PUSH socket to tcp://localhost:5558
//  Sends results to sink via that socket
//
public class taskwork
{

    public static void main(final String[] args) throws Exception
    {
        final ZMQ.Context context = ZMQ.context(1);

        // Socket to receive messages on
        final ZMQ.Socket receiver = context.socket(ZMQ.PULL);
        receiver.connect("tcp://localhost:5557");

        // Socket to send messages to
        final ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.connect("tcp://localhost:5558");

        // Process tasks forever
        while (!Thread.currentThread().isInterrupted()) {
            final String string = new String(receiver.recv(0), ZMQ.CHARSET).trim();
            final long msec = Long.parseLong(string);
            // Simple progress indicator for the viewer
            System.out.flush();
            System.out.print(string + '.');

            // Do the work
            Thread.sleep(msec);

            // Send results to sink
            sender.send(ZMQ.MESSAGE_SEPARATOR, 0);
        }
        sender.close();
        receiver.close();
        context.term();
    }
}
