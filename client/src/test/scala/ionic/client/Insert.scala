package ionic.client

import com.codahale.logula.Logging

import ionic.test.Event

import org.apache.log4j.Level

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.ChannelEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress

import org.scalatest.FunSuite

class Insert extends FunSuite {
  test("insert") {
    Logging.configure { log => log.level = Level.WARN }
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)
    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline(new Up())))
    serverBoot.setOption("localAddress", addr)
    serverBoot.bind()

    val boot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    boot.setOption("remoteAddress", addr)
    val client = new Client(boot)
    client.insert(new Event())
    client.shutdown()
  }
}

class Up extends SimpleChannelUpstreamHandler with Logging {
  override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
    super.handleUpstream(ctx, e)
    log.info("Got %s", e)
  }
}
