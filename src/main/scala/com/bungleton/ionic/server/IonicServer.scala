package com.bungleton.ionic.server

import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import java.util.concurrent.Executors
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import java.net.InetSocketAddress

object IonicServer {
  val port = 10713

  def main (args :Array[String]) {
    val boot = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
    boot.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline () = {
        // TODO - add an executor since we're doing disk IO
        Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
          new IonicServerHandler())
      }
    })
    boot.bind(new InetSocketAddress(port))
  }
}
