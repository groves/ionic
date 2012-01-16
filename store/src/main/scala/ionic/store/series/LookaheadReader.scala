package ionic.store.series

import org.apache.avro.generic.GenericRecord

object NoneFound extends Exception

abstract class LookaheadReader extends Iterator[GenericRecord] {
  private var _lookahead: Option[GenericRecord] = None
  private var _finished: Boolean = false

  def hasNext(): Boolean = !_finished && (_lookahead match {
      case Some(_) => true
      case None => {
        try {
          _lookahead = Some(read())
        } catch {
          case NoneFound => {
            _finished = true
            close()
          }
        }
        _lookahead != None
      }
    })

  def next(): GenericRecord = {
    assert(hasNext())
    val looked = _lookahead.get
    _lookahead = None
    looked
  }

  def close()
  protected def read(old :GenericRecord = null) :GenericRecord

}
