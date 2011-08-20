package ionic.client

import java.util.concurrent.ArrayBlockingQueue
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

class RecordSender(boot: ClientBootstrap) extends Actor with Logging {
  // TODO - spool overflow records to disk rather than an in-memory queue. That'll handle netork
  // outages as well
  val queue: BlockingQueue[IndexedRecord] = new ArrayBlockingQueue(1024) // Arbitrary queue size
  var nettyCapacity: Int = 0 // This will eventually come from the number of nettywriter's contexts
  var netty: NettyWriter = new NettyWriter(this, boot)
  var active: Boolean = true
  start
  def act() {
    netty.start() // Start netty now that we're ready to receive its messages
    def sendIfQueued() = {
      queue.poll() match {
        case null => ()
        case msg: IndexedRecord => {
          nettyCapacity -= 1
          netty ! msg
        }
      }
    }
    loopWhile(active) {
      react {
        case rec: IndexedRecord => {
          if (nettyCapacity > 0) {
            nettyCapacity -= 1
            netty ! rec
          } else if (!queue.offer(rec)) {
            log.warn("Overflowed message queue!")
          }
        }
        case WriterReady(_) => {
          nettyCapacity += 1
          sendIfQueued()
        }
        case Shutdown(latch) => {
          active = false
          netty ! Shutdown(latch)
        }
        case msg => log.warn("RecordSender received unknown message: %s", msg)
      }
    }
  }

  def shutdown() {
    val latch = new CountDownLatch(1)
    // TODO - drain queue
    this ! Shutdown(latch)
    latch.await(60, TimeUnit.SECONDS)
  }
}

case class Shutdown(latch: CountDownLatch)
case class WriterReady(writer: Actor)
case class WriteSucceeded(ctx: NettyMsgContext)
case class WriteFailed(ctx: NettyMsgContext, rec: IndexedRecord)

class NettyWriter(listener: Actor, boot: ClientBootstrap) extends Actor with ChannelFutureListener with Logging {
  var chan: Channel = null
  def act() {
    boot.connect().addListener(this) // Make the initial connection as we're ready to get the channel
    write()
  }

  def write(): Unit = react {
    case chan: Channel => {
      this.chan = chan
      listener ! WriterReady(this)
      write()
    }
    // TODO - cache a few contexts rather than creating every time
    case rec: IndexedRecord => {
      new NettyMsgContext(this).write(chan, rec)
      write()
    }
    case WriteSucceeded(ctx: NettyMsgContext) => {
      listener ! WriterReady(this)
      write()
    }
    // TODO - reconnect, return rec to sender for spooling
    case WriteFailed(ctx: NettyMsgContext, rec: IndexedRecord) => {
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
    reactWithin(0) {
      case rec: IndexedRecord =>
        log.warn("Asked to write a record after receiving shutdown: %s", rec)
      case Shutdown(_) =>
        log.warn("Asked to shutdown multiple times. Ignoring latch")
      case WriteSucceeded(_) =>
        close()
      case WriteFailed(_, _) =>
        close()
      case TIMEOUT =>
        close()
      case msg =>
        log.warn("NettyWriter received unknown message: %s", msg)
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

class NettyMsgContext(writer: Actor) extends ChannelFutureListener with Logging {
  val buf = ChannelBuffers.dynamicBuffer(512)
  val enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(buf), null)
  var rec: IndexedRecord = null

  def write(c: Channel, record: IndexedRecord) {
    rec = record
    enc.writeString(record.getSchema().toString())
    // TODO - cache writers per schema
    new SpecificDatumWriter(record.getSchema).write(record, enc)
    c.write(buf).addListener(this)
  }

  override def operationComplete(future: ChannelFuture) {
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
