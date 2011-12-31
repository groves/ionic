package ionic.server

import com.threerings.fisy.Paths
import java.util.concurrent.CountDownLatch

import com.codahale.logula.Logging

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender
import ionic.test.TestClientHandler

import org.apache.log4j.Level

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
    Logging.configure { log => log.level = Level.WARN }
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

    val base = Paths.makeTempFs()
    try {
      val server = new IonicServer(serverBoot, base)
      val connectFuture = clientBoot.connect(addr)
      connectFuture.awaitUninterruptibly()
      latch.await()
      connectFuture.getChannel.close()
      server.shutdown()
    } finally {
      base.delete()
    }
  }
}
