package com.bungleton.ionic.test

import java.util.concurrent.CountDownLatch
import com.bungleton.ionic.server.AvroIntFrameDecoder
import com.bungleton.ionic.server.AvroIntLengthFieldPrepender
import com.bungleton.ionic.server.IonicServer
import java.net.InetSocketAddress
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ChannelPipelineFactory
import java.util.concurrent.Executors
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import com.google.common.collect.Lists
import com.bungleton.ionic.server.IonicServer
import java.net.InetSocketAddress
import ionic.Store

object Client {
  def main (args :Array[String]) {
    val boot = new ClientBootstrap(new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
    val latch = new CountDownLatch(1)
    boot.setPipelineFactory(new ChannelPipelineFactory () {
        override def getPipeline () = {
          Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
            new TestClientHandler(latch))
        }
      })

    val connectFuture = boot.connect(new InetSocketAddress(IonicServer.port))
    val chan = connectFuture.awaitUninterruptibly().getChannel()
    latch.await()
    println("FINISHED")
    boot.releaseExternalResources()
  }
}
