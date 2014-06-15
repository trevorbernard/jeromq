/*
    Copyright (c) 2009-2011 250bpm s.r.o.
    Copyright (c) 2007-2009 iMatix Corporation
    Copyright (c) 2007-2011 Other contributors as noted in the AUTHORS file
    
    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package zmq;

//  Base class for all objects that participate in inter-thread
//  communication.
public abstract class ZObject
{

    // Context provides access to the global state.
    private final Ctx ctx;

    // Thread ID of the thread the object belongs to.
    private final int tid;

    protected ZObject(final Ctx ctx, final int tid)
    {
        this.ctx = ctx;
        this.tid = tid;
    }

    protected ZObject(final ZObject parent_)
    {
        this(parent_.ctx, parent_.tid);
    }

    protected int get_tid()
    {
        return tid;
    }

    protected Ctx get_ctx()
    {
        return ctx;
    }

    protected void process_command(final Command cmd_)
    {
        switch (cmd_.type()) {

        case ACTIVATE_READ:
            process_activate_read();
            break;

        case ACTIVATE_WRITE:
            process_activate_write((Long) cmd_.arg);
            break;

        case STOP:
            process_stop();
            break;

        case PLUG:
            process_plug();
            process_seqnum();
            break;

        case OWN:
            process_own((Own) cmd_.arg);
            process_seqnum();
            break;

        case ATTACH:
            process_attach((IEngine) cmd_.arg);
            process_seqnum();
            break;

        case BIND:
            process_bind((Pipe) cmd_.arg);
            process_seqnum();
            break;

        case HICCUP:
            process_hiccup(cmd_.arg);
            break;

        case PIPE_TERM:
            process_pipe_term();
            break;

        case PIPE_TERM_ACK:
            process_pipe_term_ack();
            break;

        case TERM_REQ:
            process_term_req((Own) cmd_.arg);
            break;

        case TERM:
            process_term((Integer) cmd_.arg);
            break;

        case TERM_ACK:
            process_term_ack();
            break;

        case REAP:
            process_reap((SocketBase) cmd_.arg);
            break;

        case REAPED:
            process_reaped();
            break;

        default:
            throw new IllegalArgumentException();
        }

    }

    protected boolean register_endpoint(final String addr_,
                                        final Ctx.Endpoint endpoint_)
    {
        return ctx.register_endpoint(addr_, endpoint_);
    }

    protected void unregister_endpoints(final SocketBase socket_)
    {
        ctx.unregister_endpoints(socket_);
    }

    protected Ctx.Endpoint find_endpoint(final String addr_)
    {
        return ctx.find_endpoint(addr_);
    }

    protected void destroy_socket(final SocketBase socket_)
    {
        ctx.destroy_socket(socket_);
    }

    // Chooses least loaded I/O thread.
    protected IOThread choose_io_thread(final long affinity_)
    {
        return ctx.choose_io_thread(affinity_);
    }

    protected void send_stop()
    {
        // 'stop' command goes always from administrative thread to
        // the current object.
        final Command cmd = new Command(this, Command.Type.STOP);
        ctx.send_command(tid, cmd);
    }

    protected void send_plug(final Own destination_)
    {
        send_plug(destination_, true);
    }

    protected void send_plug(final Own destination_, final boolean inc_seqnum_)
    {
        if (inc_seqnum_) {
            destination_.inc_seqnum();
        }

        final Command cmd = new Command(destination_, Command.Type.PLUG);
        send_command(cmd);
    }

    protected void send_own(final Own destination_, final Own object_)
    {
        destination_.inc_seqnum();
        final Command cmd = new Command(destination_, Command.Type.OWN, object_);
        send_command(cmd);
    }

    protected void send_attach(final SessionBase destination_,
                               final IEngine engine_)
    {
        send_attach(destination_, engine_, true);
    }

    protected void send_attach(final SessionBase destination_,
                               final IEngine engine_, final boolean inc_seqnum_)
    {
        if (inc_seqnum_) {
            destination_.inc_seqnum();
        }

        final Command cmd = new Command(destination_, Command.Type.ATTACH,
                                        engine_);
        send_command(cmd);
    }

    protected void send_bind(final Own destination_, final Pipe pipe_)
    {
        send_bind(destination_, pipe_, true);
    }

    protected void send_bind(final Own destination_, final Pipe pipe_,
                             final boolean inc_seqnum_)
    {
        if (inc_seqnum_) {
            destination_.inc_seqnum();
        }

        final Command cmd = new Command(destination_, Command.Type.BIND, pipe_);
        send_command(cmd);
    }

    protected void send_activate_read(final Pipe destination_)
    {
        final Command cmd = new Command(destination_,
                                        Command.Type.ACTIVATE_READ);
        send_command(cmd);
    }

    protected void send_activate_write(final Pipe destination_,
                                       final long msgs_read_)
    {
        final Command cmd = new Command(destination_,
                                        Command.Type.ACTIVATE_WRITE, msgs_read_);
        send_command(cmd);
    }

    protected void send_hiccup(final Pipe destination_, final Object pipe_)
    {
        final Command cmd = new Command(destination_, Command.Type.HICCUP,
                                        pipe_);
        send_command(cmd);
    }

    protected void send_pipe_term(final Pipe destination_)
    {
        final Command cmd = new Command(destination_, Command.Type.PIPE_TERM);
        send_command(cmd);
    }

    protected void send_pipe_term_ack(final Pipe destination_)
    {
        final Command cmd = new Command(destination_,
                                        Command.Type.PIPE_TERM_ACK);
        send_command(cmd);
    }

    protected void send_term_req(final Own destination_, final Own object_)
    {
        final Command cmd = new Command(destination_, Command.Type.TERM_REQ,
                                        object_);
        send_command(cmd);
    }

    protected void send_term(final Own destination_, final int linger_)
    {
        final Command cmd = new Command(destination_, Command.Type.TERM,
                                        linger_);
        send_command(cmd);

    }

    protected void send_term_ack(final Own destination_)
    {
        final Command cmd = new Command(destination_, Command.Type.TERM_ACK);
        send_command(cmd);
    }

    protected void send_reap(final SocketBase socket_)
    {
        final Command cmd = new Command(ctx.get_reaper(), Command.Type.REAP,
                                        socket_);
        send_command(cmd);
    }

    protected void send_reaped()
    {
        final Command cmd = new Command(ctx.get_reaper(), Command.Type.REAPED);
        send_command(cmd);
    }

    protected void send_done()
    {
        final Command cmd = new Command(null, Command.Type.DONE);
        ctx.send_command(Ctx.TERM_TID, cmd);
    }

    protected void process_stop()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_plug()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_own(final Own object)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_attach(final IEngine engine)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_bind(final Pipe pipe)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_activate_read()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_activate_write(final long msgs_read_)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_hiccup(final Object hiccup_pipe)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_pipe_term()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_pipe_term_ack()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_term_req(final Own object)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_term(final int linger)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_term_ack()
    {
        throw new UnsupportedOperationException();
    }

    protected void process_reap(final SocketBase socket)
    {
        throw new UnsupportedOperationException();
    }

    protected void process_reaped()
    {
        throw new UnsupportedOperationException();
    }

    // Special handler called after a command that requires a seqnum
    // was processed. The implementation should catch up with its counter
    // of processed commands here.
    protected void process_seqnum()
    {
        throw new UnsupportedOperationException();
    }

    private void send_command(final Command cmd_)
    {
        ctx.send_command(cmd_.destination().get_tid(), cmd_);
    }

}
