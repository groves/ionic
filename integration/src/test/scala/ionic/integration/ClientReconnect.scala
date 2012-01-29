package test.scala.ionic.integration

import com.threerings.fisy.Paths
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory
import com.threerings.fisy.impl.local.LocalDirectory
import ionic.store.EntryReader
import ionic.test.Event
import org.apache.log4j.Level
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import com.codahale.logula.Logging

import ionic.client.Client
import org.jboss.netty.bootstrap.ClientBootstrap
import ionic.server.IonicServer
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.local.LocalAddress

class ClientReconnect extends FunSuite with BeforeAndAfter {
  Logging.configure { log => log.level = Level.WARN }

  val base: LocalDirectory = Paths.makeTempFs()
  val addr = new LocalAddress(547328)

  val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
  serverBoot.setOption("localAddress", addr)
  var server = new IonicServer(serverBoot, base)

  val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
  clientBoot.setOption("remoteAddress", addr)
  val client = new Client(clientBoot)

  after {
    base.delete()
  }

  test("send 1000 records from client to server, shutdown server first") {
    (0 until 1000).foreach(_ => client.insert(new Event()))
    server.shutdown()
    assert(1000 > EntryReader("ionic.test.event", base).size)
    server = new IonicServer(serverBoot, base)
    Thread.sleep(200)
    client.shutdown()
    server.shutdown()
    // Starting a new EntryReader does a split on the data from client2
    assert(1000 === EntryReader("ionic.test.event", base).size)
  }
}
