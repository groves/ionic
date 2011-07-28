package com.bungleton.ionic.test

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelStateEvent
import java.util.concurrent.CountDownLatch
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class TestClientHandler(latch :CountDownLatch) extends SimpleChannelUpstreamHandler {
  override def channelConnected(ctx :ChannelHandlerContext, e :ChannelStateEvent) {
    val chan = e.getChannel()
    val buf = ChannelBuffers.buffer(128)
    buf.writeByte(1)
    buf.writeByte(2)
    buf.writeByte(3)
    chan.write(buf)
  }

}
