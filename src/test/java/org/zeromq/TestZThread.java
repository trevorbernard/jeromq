package org.zeromq;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ.Socket;

public class TestZThread
{

    @Test
    public void testDetached()
    {
        final ZThread.IDetachedRunnable detached = new ZThread.IDetachedRunnable()
        {

            @Override
            public void run(final Object[] args)
            {
                final ZContext ctx = new ZContext();
                assert (ctx != null);

                final Socket push = ctx.createSocket(ZMQ.PUSH);
                assert (push != null);
                ctx.destroy();
            }
        };

        ZThread.start(detached);
    }

    @Test
    public void testFork()
    {
        final ZContext ctx = new ZContext();

        final ZThread.IAttachedRunnable attached = new ZThread.IAttachedRunnable()
        {

            @Override
            public void run(final Object[] args, final ZContext ctx,
                            final Socket pipe)
            {
                // Create a socket to check it'll be automatically deleted
                ctx.createSocket(ZMQ.PUSH);
                pipe.recvStr();
                pipe.send("pong");
            }
        };

        final Socket pipe = ZThread.fork(ctx, attached);
        assert (pipe != null);

        pipe.send("ping");
        final String pong = pipe.recvStr();

        Assert.assertEquals(pong, "pong");

        // Everything should be cleanly closed now
        ctx.destroy();
    }
}
