package com.bungleton.ionic.server

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class IonicServerHandler extends SimpleChannelUpstreamHandler {
  override def messageReceived (ctx :ChannelHandlerContext, e :MessageEvent) {
    println("GOT MSG" + e.getMessage())
  }

}
