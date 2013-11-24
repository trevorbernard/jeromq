package org.zeromq.api;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Message implements Queue<Frame> {
    private final BlockingQueue<Frame> frames;

    public Message() {
        this.frames = new ArrayBlockingQueue<Frame>(8);
    }

    public static Message receiveMessage(Socket socket) {
        return null;
    }

    @Override
    public int size() {
        return frames.size();
    }

    @Override
    public boolean isEmpty() {
        return frames.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return frames.contains(o);
    }

    @Override
    public Iterator<Frame> iterator() {
        return frames.iterator();
    }

    @Override
    public Object[] toArray() {
        return frames.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return frames.toArray(a);
    }

    @Override
    public boolean remove(Object o) {
        return frames.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return frames.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Frame> c) {
        return frames.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return frames.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return frames.retainAll(c);
    }

    @Override
    public void clear() {
        frames.clear();
    }

    @Override
    public boolean add(Frame e) {
        return frames.add(e);
    }

    @Override
    public boolean offer(Frame e) {
        return frames.offer(e);
    }

    @Override
    public Frame remove() {
        return frames.remove();
    }

    @Override
    public Frame poll() {
        return frames.poll();
    }

    @Override
    public Frame element() {
        return frames.element();
    }

    @Override
    public Frame peek() {
        return frames.peek();
    }
}
