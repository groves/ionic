/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package ionic.test;  
@SuppressWarnings("all")
public class Event extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"ionic.test\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"int\"}]}");
  public long timestamp;
  public int value;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return timestamp;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: timestamp = (java.lang.Long)value$; break;
    case 1: value = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
