package com.bungleton.ionic.server

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
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

class ConnectAndSchema extends TestNGSuite {
  @Test def connectAndSchema () {
    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline()))
    serverBoot.setOption("localAddress", new LocalAddress("server"))

    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    clientBoot.setPipelineFactory(new ChannelPipelineFactory () {
      override def getPipeline () = {
        Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
          new TestClientHandler())
      }
    })


    val server = new IonicServer(serverBoot, IonicServer.createTempDirectory())
    val connectFuture = clientBoot.connect(new LocalAddress("server"))
    connectFuture.awaitUninterruptibly()
    clientBoot.releaseExternalResources()
    server.close().awaitUninterruptibly()
    println("Finished")
  }
}
