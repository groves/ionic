package com.bungleton.ionic.server

import org.scalatest.FunSuite
import java.util.concurrent.CountDownLatch
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import com.threerings.fisy.impl.local.LocalDirectory
import java.io.File
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ChannelPipelineFactory
import java.util.concurrent.CountDownLatch
import com.bungleton.ionic.test.TestClientHandler
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.LocalAddress
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory

class ConnectAndSchema extends FunSuite {
  test("connect and send schema") {
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)
    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline()))
    serverBoot.setOption("localAddress", addr)

    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    val latch = new CountDownLatch(1)
    clientBoot.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline() = {
        Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
          new TestClientHandler(latch))
      }
    })

    val server = new IonicServer(serverBoot, IonicServer.createTempDirectory())
    val connectFuture = clientBoot.connect(addr)
    connectFuture.awaitUninterruptibly()
    latch.await()
    connectFuture.getChannel.close()
    server.close().awaitUninterruptibly()
  }
}
