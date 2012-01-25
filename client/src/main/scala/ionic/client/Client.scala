package ionic.client

import java.net.InetSocketAddress
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors

import scala.actors.Actor.actor

import com.codahale.logula.Logging

import com.google.common.util.concurrent.ThreadFactoryBuilder

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender

import org.apache.avro.generic.IndexedRecord

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory

object Client {
  private def makeBootstrap(host: String, port: Int): ClientBootstrap = {
    val fact = new ThreadFactoryBuilder().setNameFormat("IonicClient-%s").setDaemon(true).build()
    val exec: Executor = Executors.newSingleThreadExecutor(fact)
    val boot: ClientBootstrap = new ClientBootstrap(new OioClientSocketChannelFactory(exec))
    boot.setOption("remoteAddress", new InetSocketAddress(host, port))
    boot
  }
}

class Client(private val boot: ClientBootstrap) extends Logging {
  def this(host: String, port: Int) = this(Client.makeBootstrap(host, port))
  val queue: BlockingQueue[IndexedRecord] = new ArrayBlockingQueue(1024)
  val mapper: SchemaMapper = new SchemaMapper()
  boot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline(
    new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(), mapper)))

  val sender = new RecordSender(queue, boot, mapper)

  def errored() = mapper.errored

  def insert(record: IndexedRecord, waitForSending: Boolean = false) {
    if (mapper.errored) return // Mapper warned about it
    if (waitForSending) queue.put(record)
    else if (!queue.offer(record)) {
      log.warn("Queue overflowed")
      return
    }
    sender ! QueueInserted
  }

  def shutdown() { sender.shutdown() }
}
