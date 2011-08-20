package ionic.server

import java.util.concurrent.CountDownLatch

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender
import ionic.test.TestClientHandler

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress

import org.scalatest.FunSuite

class ConnectAndSchema extends FunSuite {
  test("connect and send schema") {
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)
    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
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
