import sbt._
import Keys._
import com.typesafe.sbtscalariform.ScalariformPlugin

object IonicBuild extends Build {
  // common build configuration
  val buildSettings = Defaults.defaultSettings ++ ScalariformPlugin.settings ++ Seq(
    organization := "com.bungleton.ionic",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.9.0-1",
    scalacOptions ++= Seq("-deprecation"),
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "11.0.1",
      "com.codahale" %% "logula" % "2.1.3",
      "org.slf4j" % "slf4j-simple" % "1.6.1",
      "org.apache.avro" % "avro" % "1.5.1",
      "org.jboss.netty" % "netty" % "3.2.4.Final",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository",
      "codahale" at "http://repo.codahale.com")
  )


  def sub (id :String, subSettings :Seq[Setting[_]] = Seq()) = Project(id, file(id),
    settings = buildSettings ++ subSettings ++ Seq(name := id))

  lazy val net = sub("net")

  lazy val client = sub("client") dependsOn(net)

  lazy val store = sub("store", Seq(
    libraryDependencies ++= Seq("com.threerings" % "fisy" % "1.0-SNAPSHOT",
        "com.threerings" % "react" % "1.2-SNAPSHOT",
        "org.xerial.snappy" % "snappy-java" % "1.0.4.1")))

  lazy val server = sub("server") dependsOn(net, store)

  lazy val integration = sub("integration") dependsOn(server, client)

  lazy val ionic = Project("ionic", file(".")) aggregate(net, client, store, server, integration)
}
