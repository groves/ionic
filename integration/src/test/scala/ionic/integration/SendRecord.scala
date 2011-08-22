package ionic.integration

import ionic.store.EntryReader
import com.google.common.collect.Iterables
import ionic.client.Client
import ionic.server.IonicServer
import ionic.test.Event

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress

import org.scalatest.FunSuite

class SendRecord extends FunSuite {
  test("send 1000 records from client to server") {
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)

    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setOption("localAddress", addr)
    val base = IonicServer.createTempDirectory()
    val server = new IonicServer(serverBoot, base)

    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    clientBoot.setOption("remoteAddress", addr)
    val client = new Client(clientBoot)
    (0 until 1000).foreach(_ => client.insert(new Event()))
    client.shutdown()
    server.shutdown()
    assert(1000 === Iterables.size(new EntryReader("ionic.test.event", base)))
  }
}
