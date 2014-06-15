package guide;

import org.zeromq.ZMQ;

/**
 * Task sink - design 2 Adds pub-sub flow to send kill signal to workers
 */
public class tasksink2
{

    public static void main(final String[] args) throws Exception
    {

        // Prepare our context and socket
        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket receiver = context.socket(ZMQ.PULL);
        receiver.bind("tcp://*:5558");

        // Socket for worker control
        final ZMQ.Socket controller = context.socket(ZMQ.PUB);
        controller.bind("tcp://*:5559");

        // Wait for start of batch
        receiver.recv(0);

        // Start our clock now
        final long tstart = System.currentTimeMillis();

        // Process 100 confirmations
        int task_nbr;
        for (task_nbr = 0; task_nbr < 100; task_nbr++) {
            receiver.recv(0);
            if ((task_nbr / 10) * 10 == task_nbr) {
                System.out.print(":");
            }
            else {
                System.out.print(".");
            }
            System.out.flush();
        }
        // Calculate and report duration of batch
        final long tend = System.currentTimeMillis();

        System.out.println("Total elapsed time: " + (tend - tstart) + " msec");

        // Send the kill signal to the workers
        controller.send("KILL", 0);

        // Give it some time to deliver
        Thread.sleep(1);

        receiver.close();
        controller.close();
        context.term();
    }
}
