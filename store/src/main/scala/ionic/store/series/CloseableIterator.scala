package ionic.store.series

import java.io.Closeable

trait CloseableIterator[T] extends Iterator[T] with Closeable {}
