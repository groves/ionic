package ionic.integration

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

import com.threerings.fisy.Directory
import com.threerings.fisy.Paths
import com.threerings.fisy.impl.local.LocalDirectory

class SendRecord extends FunSuite with OneInstancePerTest with BeforeAndAfter {
  Logging.configure { log => log.level = Level.WARN }
  val base: LocalDirectory = Paths.makeTempFs()

  val addr = new LocalAddress(LocalAddress.EPHEMERAL)

  val serverBoot = new ServerBootstrap(new DefaultLocalServerChannelFactory())
  serverBoot.setOption("localAddress", addr)
  var server: IonicServer = new IonicServer(serverBoot, base)

  val clientBoot = new ClientBootstrap(new DefaultLocalClientChannelFactory())
  clientBoot.setOption("remoteAddress", addr)
  val client = new Client(clientBoot)

  after {
    base.delete()
  }

  test("send 1000 records from client to server") {
    (0 until 1000).foreach(_ => client.insert(new Event()))
    client.shutdown()
    server.shutdown()
    assert(1000 === EntryReader("ionic.test.event", base).size)
  }

  test("send 5000 records from client to server at write rate") {
    (0 until 5000).foreach(_ => client.insert(new Event(), waitForSending = true))
    client.shutdown()
    server.shutdown()
    assert(5000 === EntryReader("ionic.test.event", base).size)
  }

  test("attempt write on misconfigured server") {
    Logging.configure { log => log.level = Level.ERROR }
    base.file.setWritable(false)
    (0 until 5).foreach(_ => client.insert(new Event()))
    client.shutdown()
    server.shutdown()
    base.file.setWritable(true)
    assert(0 === EntryReader("ionic.test.event", base).size)
    assert(client.errored)
  }
}
