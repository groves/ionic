import sbt._
import org.apache.avro.compiler.specific.SpecificCompiler

object AvroPlugin extends Plugin {
  val genavro = TaskKey[Unit]("genavro", "Generates Java from avro schemas")

  val genavroTask = genavro := {
    List("src/main", "src/test").map(file).foreach(base => {
      (base / "avro" ** "*.avsc").get.foreach(s => SpecificCompiler.compileSchema(s, base / "java"))
    })
  }

  override val settings = Seq(genavroTask)
}
