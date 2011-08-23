package ionic.client

import java.util.concurrent.BlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.actors.Actor
import scala.actors.TIMEOUT

import com.codahale.logula.Logging

import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener

case class QueueInserted()
class RecordSender(queue: BlockingQueue[IndexedRecord], boot: ClientBootstrap, mapper: SchemaMapper) extends Actor with Logging {
  val netty: NettyWriter = new NettyWriter(this, boot, mapper)
  var capacity: Int = 0
  start

  def sendFromQueue(): Boolean = queue.poll() match {
    case null => false
    case rec => {
      capacity -= 1
      netty ! rec
      true
    }
  }

  def act() = react {
    case QueueInserted => {
      if (capacity > 0) {
        sendFromQueue()
      }
      act()
    }
    case WriterReady(_) => {
      capacity += 1
      sendFromQueue()
      act()
    }
    case Shutdown(latch) => {
      drain(latch)
    }
    case msg => {
      log.warn("RecordSender received unknown message: %s", msg)
      act()
    }
  }

  def drain(latch: CountDownLatch): Unit = reactWithin(10000) {
    case QueueInserted => {
      log.warn("Got queue insertion after being asked to shutdown")
      drain(latch)
    }
    case WriterReady(_) => {
      capacity += 1
      if (!sendFromQueue()) {
        netty ! Shutdown(latch)
      } else {
        drain(latch)
      }
    }
    case TIMEOUT => {
      log.warn("Didn't get a writer ready within 10 seconds, shutting down without finishing draining queue")
      netty ! Shutdown(latch)
    }
    case Shutdown(latch) => {
      log.warn("Asked to shutdown multiple times")
      drain(latch)
    }
    case msg => {
      log.warn("RecordSender received unknown message: %s", msg)
      drain(latch)
    }
  }

  def shutdown() {
    val latch = new CountDownLatch(1)
    this ! Shutdown(latch)
    latch.await(60, TimeUnit.SECONDS)
  }
}

case class Shutdown(latch: CountDownLatch)
case class WriterReady(writer: Actor)
case class WriteSucceeded(ctx: NettyMsgContext)
case class WriteFailed(ctx: NettyMsgContext, rec: IndexedRecord)

class NettyWriter(listener: Actor, boot: ClientBootstrap, mapper: SchemaMapper) extends Actor with ChannelFutureListener with Logging {
  var chan: Channel = null
  var ctxs: List[NettyMsgContext] = List(0 until 10).map(_ => new NettyMsgContext(this, mapper))
  val initialLength = ctxs.length
  start
  def act() {
    boot.connect().addListener(this) // Make the initial connection as we're ready to get the channel
    write()
  }

  def write(): Unit = react {
    case chan: Channel => {
      this.chan = chan
      ctxs.foreach(_ => listener ! WriterReady(this))
      write()
    }
    case rec: IndexedRecord => {
      ctxs.head.write(chan, rec)
      ctxs = ctxs.tail
      write()
    }
    case WriteSucceeded(ctx) => {
      ctxs = ctx :: ctxs
      listener ! WriterReady(this)
      write()
    }
    // TODO - reconnect, return rec to sender for spooling
    case WriteFailed(ctx, rec) => {
      ctxs = ctx :: ctxs
      listener ! WriterReady(this)
      write()
    }
    case Shutdown(latch) =>
      drain(latch)
    case msg =>
      log.warn("NettyWriter received unknown message: %s", msg)
  }

  def drain(latch: CountDownLatch): Unit = {
    def close() = {
      if (chan != null) {
        chan.close()
      }
      latch.countDown()
    }
    if (ctxs.length == initialLength) {
      close()
      return
    }
    reactWithin(10000) {
      case rec: IndexedRecord =>
        log.warn("Asked to write a record after receiving shutdown: %s", rec)
        drain(latch)
      case Shutdown(_) =>
        log.warn("Asked to shutdown multiple times. Ignoring additonal latch.")
        drain(latch)
      case WriteSucceeded(ctx) =>
        ctxs = ctx :: ctxs
        drain(latch)
      case WriteFailed(ctx, _) =>
        ctxs = ctx :: ctxs
        drain(latch)
      case TIMEOUT =>
        log.warn("Timed out waiting for messages to finish sending. Closing anyway.")
        close()
      case msg =>
        log.warn("NettyWriter received unknown message: %s", msg)
        drain(latch)
    }
  }

  override def operationComplete(future: ChannelFuture) {
    if (future.isSuccess()) {
      this ! future.getChannel
    } else if (future.getCause() != null) {
      log.warn(future.getCause(), "Failed to connect to ionic")
    } else {
      log.warn("Ionic connection cancelled?")
    }
  }
}

class NettyMsgContext(writer: Actor, mapper: SchemaMapper) extends ChannelFutureListener with Logging {
  val buf = ChannelBuffers.dynamicBuffer(512)
  val enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(buf), null)
  var rec: IndexedRecord = null

  def write(c: Channel, record: IndexedRecord) {
    rec = record
    mapper(record.getSchema().getFullName()) match {
      case Some(idx) => {
        enc.writeInt(1)
        enc.writeLong(idx)
      }
      case None => {
        enc.writeInt(0)
        enc.writeString(record.getSchema().toString())
      }
    }
    // TODO - cache writers per schema
    new SpecificDatumWriter(record.getSchema).write(record, enc)
    c.write(buf).addListener(this)
  }

  override def operationComplete(future: ChannelFuture) {
    buf.clear()
    if (future.isSuccess()) {
      writer ! WriteSucceeded(this)
    } else if (future.getCause() != null) { // Failure! Dreaded, inevitable failure!
      log.warn(future.getCause(), "Write operation failed!")
      writer ! WriteFailed(this, rec)
    } else { // Cancelled? Uhh, who's calling cancel on our futures?
      log.warn("Got something other than failure or success to a write? [cancelled=%s]",
        future.isCancelled)
    }
  }
}
