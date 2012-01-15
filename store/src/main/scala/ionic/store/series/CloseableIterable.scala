package ionic.store.series

import java.io.Closeable

trait CloseableIterable[T] extends Iterable[T] {
  def iterator :CloseableIterator[T];
}
