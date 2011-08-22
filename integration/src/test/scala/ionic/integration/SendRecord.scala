package ionic.integration

import com.codahale.logula.Logging
import org.apache.log4j.Level

import com.threerings.fisy.Directory
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
  def createClientAndServer(): (Directory, Client, IonicServer) = {
    Logging.configure { log => log.level = Level.INFO }
    val addr = new LocalAddress(LocalAddress.EPHEMERAL)

    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setOption("localAddress", addr)
    val base = IonicServer.createTempDirectory()
    val server = new IonicServer(serverBoot, base)

    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    clientBoot.setOption("remoteAddress", addr)

    (base, new Client(clientBoot), server)
  }

  test("send 1000 records from client to server") {
    val (base, client, server) = createClientAndServer()
    (0 until 1000).foreach(_ => client.insert(new Event()))
    client.shutdown()
    server.shutdown()
    assert(1000 === Iterables.size(new EntryReader("ionic.test.event", base)))
  }

  test("send 5000 records from client to server at write rate") {
    val (base, client, server) = createClientAndServer()
    (0 until 5000).foreach(_ => client.insert(new Event(), waitForSending = true))
    client.shutdown()
    server.shutdown()
    assert(5000 === Iterables.size(new EntryReader("ionic.test.event", base)))
  }
}
