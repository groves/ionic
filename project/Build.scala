import sbt._
import Keys._

object IonicBuild extends Build {
  // common build configuration
  val buildSettings = Defaults.defaultSettings ++ ScalariformPlugin.settings ++ Seq(
    organization     := "com.bungleton",
    version          := "0.1-SNAPSHOT",
    scalaVersion := "2.9.0-1",
    resolvers        += "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository",
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "r09",
      "org.apache.avro" % "avro" % "1.5.1",
      "org.jboss.netty" % "netty" % "3.2.4.Final",
      "org.scalatest" % "scalatest_2.9.0-1" % "1.6.1" % "test"
    ))


  def sub (id :String, subSettings :Seq[Setting[_]] = Seq()) = Project(id, file(id),
    settings = buildSettings ++ subSettings ++ Seq(name := "ionic-" + id))

  lazy val client = sub("client")

  lazy val store = sub("store", Seq(
    libraryDependencies ++= Seq("com.threerings" % "fisy" % "1.0-SNAPSHOT")))

  lazy val server = sub("server") dependsOn(client, store)

  lazy val ionic = Project("ionic", file(".")) aggregate(client, store, server)
}
