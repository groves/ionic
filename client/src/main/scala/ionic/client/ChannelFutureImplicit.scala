package ionic.client

import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener

object ChannelFutureListenerImplicit {
  implicit def funToListener(f: (ChannelFuture) => Unit) =
    new ChannelFutureListener() { def operationComplete(future :ChannelFuture) { f(future) } }
}
