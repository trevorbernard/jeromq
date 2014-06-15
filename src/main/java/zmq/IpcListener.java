/*
    Copyright (c) 2011 250bpm s.r.o.
    Copyright (c) 2011 Other contributors as noted in the AUTHORS file

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

import java.net.InetSocketAddress;

// fake Unix domain socket
public class IpcListener extends TcpListener
{

    private final IpcAddress address;

    public IpcListener(final IOThread io_thread_, final SocketBase socket_,
                       final Options options_)
    {
        super(io_thread_, socket_, options_);

        address = new IpcAddress();
    }

    // Get the bound address for use with wildcards
    @Override
    public String get_address()
    {
        return address.toString();
    }

    // Set address to listen on.
    @Override
    public int set_address(final String addr_)
    {

        address.resolve(addr_, false);

        final InetSocketAddress sock = (InetSocketAddress) address.address();
        final String fake = sock.getAddress().getHostAddress() + ":"
                            + sock.getPort();
        return super.set_address(fake);
    }

}
