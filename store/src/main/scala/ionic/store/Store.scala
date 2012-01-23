package ionic.store

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache

import ionic.query.Query
import ionic.store.series.CloseableIterable
import ionic.store.series.SeriesParceler
import ionic.store.series.UnitedSeriesWriter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.impl.local.LocalDirectory

class Store (base :LocalDirectory) {
  private val parcelers :LoadingCache[String, SeriesParceler] =
    CacheBuilder.newBuilder().build(new CacheLoader[String, SeriesParceler](){
      def load(key :String) :SeriesParceler = new SeriesParceler(base, key)
    })
  def writer(s :Schema) :UnitedSeriesWriter = parcelers.get(s.getFullName).writer(s)
  def reader(query :String) :CloseableIterable[GenericRecord] = {
    val parsed = Query.parse(query)
    parcelers.get(parsed.from).reader(parsed)
  }
}
