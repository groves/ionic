package ionic.integration

import com.codahale.logula.Logging

import com.google.common.collect.Iterables

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

class SendRecord extends FunSuite with OneInstancePerTest with BeforeAndAfter {
  Logging.configure { log => log.level = Level.INFO }

  val base: Directory = IonicServer.createTempDirectory()

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
    assert(1000 === Iterables.size(new EntryReader("ionic.test.event", base)))
  }

  test("send 5000 records from client to server at write rate") {
    (0 until 5000).foreach(_ => client.insert(new Event(), waitForSending = true))
    client.shutdown()
    server.shutdown()
    assert(5000 === Iterables.size(new EntryReader("ionic.test.event", base)))
  }
}
