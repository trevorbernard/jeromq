package org.zeromq.api;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

public class Socket implements Closeable {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final SocketBase base;
    private final SocketType type;

    public Socket(SocketBase base, SocketType type) {
        this.base = base;
        this.type = type;
    }

    public SocketType getSocketType() {
        return type;
    }

    public boolean connect(String endpoint) {
        return base.connect(endpoint);
    }

    public boolean disconnect(String endpoint) {
        return base.term_endpoint(endpoint);
    }

    public boolean bind(String endpoint) {
        return base.bind(endpoint);
    }

    public boolean unbind(String endpoint) {
        return base.term_endpoint(endpoint);
    }

    public boolean hasReceiveMore() {
        return base.getsockopt(zmq.ZMQ.ZMQ_RCVMORE) == 1;
    }

    public byte[] receive() {
        return receive(SocketFlag.NONE);
    }

    public byte[] receive(SocketFlag flag) {
        Msg msg = base.recv(flag.getValue());
        return msg.data();
    }

    public int send(byte[] buf) {
        return send(buf, EnumSet.noneOf(SocketFlag.class));
    }

    public int send(byte[] buf, SocketFlag flag) {
        return send(buf, EnumSet.of(flag));
    }

    private int getFlagsMask(EnumSet<SocketFlag> flags) {
        int flagsMask = 0;
        for (SocketFlag f : flags)
            flagsMask |= f.getValue();
        return flagsMask;
    }

    public int send(byte[] buf, EnumSet<SocketFlag> flags) {
        Msg msg = new Msg(buf);
        int flagsMask = getFlagsMask(flags);
        base.send(msg, flagsMask);
        return msg.size();
    }

    public int sendByteBuffer(ByteBuffer buf) {
        Msg msg = new Msg(buf);
        base.send(msg, 0);
        return msg.size();
    }

    public int sendByteBuffer(ByteBuffer buf, EnumSet<SocketFlag> flags) {
        Msg msg = new Msg(buf);
        int flagsMask = getFlagsMask(flags);
        base.send(msg, flagsMask);
        return msg.size();
    }

    public int sendFrame(Frame frame) {
        return this.send(frame.getData(), frame.hasMore() ? SocketFlag.SEND_MORE : SocketFlag.NONE);
    }

    public Frame receiveFrame() {
        return receiveFrame(SocketFlag.NONE);
    }

    public Frame receiveFrame(SocketFlag flag) {
        byte[] data = this.receive(flag);
        return Frame.createFrame(data, this.hasReceiveMore(), false);
    }

    public Message receiveMessage() {
        Message message = new Message();
        do {
            Frame frame = this.receiveFrame();
            System.out.println(frame);
            message.add(frame);
        } while (!Thread.currentThread().isInterrupted() && this.hasReceiveMore());
        return message;
    }

    public int sendMessage(Message message) {
        int size = 0;
        for (Frame frame : message) {
            size += frame.size();
            this.sendFrame(frame);
            System.out.println("Send: " + frame);
        }
        return size;
    }

    public int getIPv4Only() {
        return base.getsockopt(ZMQ.ZMQ_IPV4ONLY);
    }

    public int getSendHighWaterMark() {
        return base.getsockopt(ZMQ.ZMQ_SNDHWM);
    }

    public int getReceiveHighWaterMark() {
        return base.getsockopt(ZMQ.ZMQ_RCVHWM);
    }

    public int getAffinity() {
        return base.getsockopt(ZMQ.ZMQ_AFFINITY);
    }

    public byte[] getIdentity() {
        return (byte[]) base.getsockoptx(ZMQ.ZMQ_IDENTITY);
    }

    public int getRate() {
        return base.getsockopt(ZMQ.ZMQ_RATE);
    }

    public int getRecoveryIVL() {
        return base.getsockopt(ZMQ.ZMQ_RECOVERY_IVL);
    }

    public int getSendBuffer() {
        return base.getsockopt(ZMQ.ZMQ_SNDBUF);
    }

    public int getRcvbuf() {
        return base.getsockopt(ZMQ.ZMQ_RCVBUF);
    }

    public int getLinger() {
        return base.getsockopt(ZMQ.ZMQ_LINGER);
    }

    public int getReconnectIVL() {
        return base.getsockopt(ZMQ.ZMQ_RECONNECT_IVL);
    }

    public int getReconnectIVLMax() {
        return base.getsockopt(ZMQ.ZMQ_RECONNECT_IVL_MAX);
    }

    public int getBacklog() {
        return base.getsockopt(ZMQ.ZMQ_BACKLOG);
    }

    public int getMaxMessageSize() {
        return base.getsockopt(ZMQ.ZMQ_MAXMSGSIZE);
    }

    public int getMulticastHops() {
        return base.getsockopt(ZMQ.ZMQ_MULTICAST_HOPS);
    }

    public int getReceiveTimeout() {
        return base.getsockopt(ZMQ.ZMQ_RCVTIMEO);
    }

    public int getSendTimeout() {
        return base.getsockopt(ZMQ.ZMQ_SNDTIMEO);
    }

    public int getTCPKeepalive() {
        return base.getsockopt(ZMQ.ZMQ_TCP_KEEPALIVE);
    }

    public int getTCPKeepaliveIdle() {
        return base.getsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_IDLE);

    }

    public int getTCPKeepaliveCnt() {
        return base.getsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_CNT);

    }

    public int getTCPKeepaliveIntvl() {
        return base.getsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_INTVL);

    }

    public byte[] getTCPAcceptFilter() {
        return (byte[]) base.getsockoptx(ZMQ.ZMQ_TCP_ACCEPT_FILTER);

    }

    public int getEvents() {
        return base.getsockopt(ZMQ.ZMQ_EVENTS);

    }

    public String getLastEndpoint() {
        byte[] b = (byte[]) base.getsockoptx(ZMQ.ZMQ_LAST_ENDPOINT);
        return new String(b, UTF8);
    }

    // Set socket options
    public void setRouterRaw(boolean routerRaw) {
        // Currently not implemented
        // base.setsockopt(ZMQ.ZMQ_ROUTER_RAW, routerRaw ? 1 : 0);
        throw new UnsupportedOperationException("Currently not implemented");
    }

    public void setIPv4Only(boolean ipv4Only) {
        base.setsockopt(ZMQ.ZMQ_IPV4ONLY, ipv4Only ? 1 : 0);
    }

    public void setDelayAttachOnConnect(boolean delayAttachOnConnect) {
        base.setsockopt(ZMQ.ZMQ_DELAY_ATTACH_ON_CONNECT, delayAttachOnConnect ? 1 : 0);
    }

    public void setSendHighWaterMark(int sndhwm) {
        base.setsockopt(ZMQ.ZMQ_SNDHWM, sndhwm);
    }

    public void setReceiveHighWaterMark(int rcvhwm) {
        base.setsockopt(ZMQ.ZMQ_RCVHWM, rcvhwm);
    }

    public void setAffinity(int affinity) {
        base.setsockopt(ZMQ.ZMQ_AFFINITY, affinity);
    }

    public void subscribe(byte[] topic) {
        base.setsockopt(ZMQ.ZMQ_SUBSCRIBE, topic);
    }

    public void unsubscribe(byte[] topic) {
        base.setsockopt(ZMQ.ZMQ_SUBSCRIBE, topic);
    }

    public void setIdentity(byte[] identity) {
        base.setsockopt(ZMQ.ZMQ_IDENTITY, identity);
    }

    public void setRate(int rate) {
        base.setsockopt(ZMQ.ZMQ_RATE, rate);
    }

    public void setRecoveryIVL(int recoveryIVL) {
        base.setsockopt(ZMQ.ZMQ_RECOVERY_IVL, recoveryIVL);
    }

    public void setSendBuffer(int sendBuffer) {
        base.setsockopt(ZMQ.ZMQ_SNDBUF, sendBuffer);
    }

    public void setReceiveBuffer(int rcvbuf) {
        base.setsockopt(ZMQ.ZMQ_RCVBUF, rcvbuf);
    }

    public void setLinger(int linger) {
        base.setsockopt(ZMQ.ZMQ_LINGER, linger);
    }

    public void setReconnectIVL(int reconnectIVL) {
        base.setsockopt(ZMQ.ZMQ_RECONNECT_IVL, reconnectIVL);
    }

    public void setReconnectIVLMax(int reconnectIVLMax) {
        base.setsockopt(ZMQ.ZMQ_RECONNECT_IVL_MAX, reconnectIVLMax);
    }

    public void setBacklog(int backlog) {
        base.setsockopt(ZMQ.ZMQ_BACKLOG, backlog);
    }

    public void setMaxMessageSize(int maxmsgsize) {
        base.setsockopt(ZMQ.ZMQ_MAXMSGSIZE, maxmsgsize);
    }

    public void setMulticastHops(int multicastHops) {
        base.setsockopt(ZMQ.ZMQ_MULTICAST_HOPS, multicastHops);
    }

    public void setReceiveTimeout(int rcvtimeo) {
        base.setsockopt(ZMQ.ZMQ_RCVTIMEO, rcvtimeo);
    }

    public void setSendTimeout(int sndtimeo) {
        base.setsockopt(ZMQ.ZMQ_SNDTIMEO, sndtimeo);
    }

    public void setXPUBVerbose(boolean xpubVerbose) {
        base.setsockopt(ZMQ.ZMQ_XPUB_VERBOSE, xpubVerbose ? 1 : 0);
    }

    public void setTCPKeepalive(int tcpKeepalive) {
        base.setsockopt(ZMQ.ZMQ_TCP_KEEPALIVE, tcpKeepalive);
    }

    public void setTCPKeepaliveIdle(int tcpKeepaliveIdle) {
        base.setsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_IDLE, tcpKeepaliveIdle);
    }

    public void setTCPKeepaliveCnt(int tcpKeepaliveCnt) {
        base.setsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_CNT, tcpKeepaliveCnt);
    }

    public void setTCPKeepaliveIntvl(int tcpKeepaliveIntvl) {
        base.setsockopt(ZMQ.ZMQ_TCP_KEEPALIVE_INTVL, tcpKeepaliveIntvl);
    }

    public void setTCPAcceptFilter(byte[] tcpAcceptFilter) {
        base.setsockopt(ZMQ.ZMQ_TCP_ACCEPT_FILTER, tcpAcceptFilter);
    }

    public void setHighWaterMark(int hwm) {
        setSendHighWaterMark(hwm);
        setReceiveHighWaterMark(hwm);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            base.close();
        }
    }
}
