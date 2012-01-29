package test.scala.ionic.integration

import com.codahale.logula.Logging

import ionic.client.Client
import ionic.server.IonicServer
import ionic.store.EntryReader
import ionic.test.Event

import org.apache.log4j.Level

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.jboss.netty.channel.local.LocalAddress

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.OneInstancePerTest

import com.threerings.fisy.Paths
import com.threerings.fisy.impl.local.LocalDirectory

class ClientReconnect extends FunSuite with OneInstancePerTest with BeforeAndAfter {
  Logging.configure { log => log.level = Level.WARN }

  val base: LocalDirectory = Paths.makeTempFs()

  def createServer(port :Int) = {
    val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBoot.setOption("localAddress", new LocalAddress(port))
    new IonicServer(serverBoot, base)
  }

  def createClient(port :Int) = {
    val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
    clientBoot.setOption("remoteAddress", new LocalAddress(port))
    new Client(clientBoot)
  }

  after {
    base.delete()
  }

  test("send 1000 records from client to server, shutdown server first") {
    var server = createServer(547328)
    val client = createClient(547328)
    (0 until 1000).foreach(_ => client.insert(new Event()))
    server.shutdown()
    assert(1000 > EntryReader("ionic.test.event", base).size)
    server = createServer(547328)
    Thread.sleep(1100)// TODO - actually resume based on some event...
    client.shutdown()
    server.shutdown()
    assert(1000 === EntryReader("ionic.test.event", base).size)
  }

  test("send 1000 records with client started before server") {
    val client = createClient(547329)
    (0 until 1000).foreach(_ => client.insert(new Event()))
    var server = createServer(547329)
    Thread.sleep(1100)// TODO - actually resume based on some event...
    client.shutdown()
    server.shutdown()
    assert(1000 === EntryReader("ionic.test.event", base).size)
  }
}
