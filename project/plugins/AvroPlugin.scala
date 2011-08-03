import sbt._
import org.apache.avro.compiler.specific.SpecificCompiler

object AvroPlugin extends Plugin {
  val genavro = TaskKey[Unit]("genavro", "Generates Java from avro schemas")

  val genavroTask = genavro := {
    (file("src/main/avro") ** "*.avsc").get.foreach(s =>
      SpecificCompiler.compileSchema(s, file("src/main/java")))
  }

  override val settings = Seq(genavroTask)
}
