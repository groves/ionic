package ionic.integration

import ionic.test.Event
import ionic.server.IonicServer
import ionic.client.Client
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.local.LocalAddress
import org.scalatest.FunSuite

class SendRecord extends FunSuite {
  test("send a single record from client to server") {
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)

    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setOption("localAddress", addr)
    val server = new IonicServer(serverBoot, IonicServer.createTempDirectory())

    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    clientBoot.setOption("remoteAddress", addr)
    val client = new Client(clientBoot)
    client.insert(new Event())
    client.shutdown()
    server.close().awaitUninterruptibly()

  }
}
