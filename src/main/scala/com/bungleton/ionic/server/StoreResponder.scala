package com.bungleton.ionic.server

import org.apache.avro.Protocol
import org.apache.avro.ipc.Transceiver
import org.apache.avro.ipc.specific.SpecificResponder
import ionic.Store
import java.util.{List => JList}
import java.nio.ByteBuffer
import org.apache.avro.ipc.Responder

class StoreResponder extends SpecificResponder(classOf[Store], null) {

  override def respond(buffers :JList[ByteBuffer], conn :Transceiver) :JList[ByteBuffer] = {
    super.respond(buffers, conn)
  }


  override def respond(message :Protocol#Message, request: Object) :Object = {
    message.getName match {
      case "defineSchemas" => "def'd"
      case "store" => "store'd"
    }
  }
}
