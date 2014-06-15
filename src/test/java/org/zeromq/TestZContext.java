package org.zeromq;

import org.junit.Assert;
import org.junit.Test;

public class TestZContext
{

    @Test
    public void testZContext()
    {
        final ZContext ctx = new ZContext();
        ctx.createSocket(ZMQ.PAIR);
        ctx.createSocket(ZMQ.XREQ);
        ctx.createSocket(ZMQ.REQ);
        ctx.createSocket(ZMQ.REP);
        ctx.createSocket(ZMQ.PUB);
        ctx.createSocket(ZMQ.SUB);
        ctx.close();
        Assert.assertEquals(0, ctx.getSockets().size());
    }
}
