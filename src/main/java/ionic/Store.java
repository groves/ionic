/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package ionic;

@SuppressWarnings("all")
public interface Store {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"Store\",\"namespace\":\"ionic\",\"types\":[],\"messages\":{\"defineSchemas\":{\"request\":[{\"name\":\"schemas\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}],\"response\":\"string\"},\"store\":{\"request\":[{\"name\":\"schema\",\"type\":\"int\"},{\"name\":\"record\",\"type\":\"bytes\"}],\"response\":\"string\"}}}");
  java.lang.CharSequence defineSchemas(java.util.List<java.lang.CharSequence> schemas) throws org.apache.avro.AvroRemoteException;
  java.lang.CharSequence store(int schema, java.nio.ByteBuffer record) throws org.apache.avro.AvroRemoteException;
}