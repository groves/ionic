import sbt._
import Keys._

object IonicBuild extends Build {
  // common build configuration
  val buildSettings = Defaults.defaultSettings ++ ScalariformPlugin.settings ++ Seq(
    organization := "com.bungleton",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.9.0-1",
    scalacOptions ++= Seq("-deprecation"),
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "r09",
      "com.codahale" %% "logula" % "2.1.3",
      "org.slf4j" % "slf4j-simple" % "1.6.1",
      "org.apache.avro" % "avro" % "1.5.1",
      "org.jboss.netty" % "netty" % "3.2.4.Final",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test"
    ),

    resolvers ++= Seq(
      "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository",
      "codahale" at "http://repo.codahale.com"),


    // this hackery causes publish-local to install to ~/.m2/repository instead of ~/.ivy
    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
    publishLocalConfiguration <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, level)
    }
  )


  def sub (id :String, subSettings :Seq[Setting[_]] = Seq()) = Project(id, file(id),
    settings = buildSettings ++ subSettings ++ Seq(name := "ionic-" + id))

  lazy val net = sub("net")

  lazy val client = sub("client") dependsOn(net)

  lazy val store = sub("store", Seq(
    libraryDependencies ++= Seq("com.threerings" % "fisy" % "1.0-SNAPSHOT")))

  lazy val server = sub("server") dependsOn(net, store)

  lazy val integration = sub("integration") dependsOn(server, client)

  lazy val ionic = Project("ionic", file(".")) aggregate(net, client, store, server, integration)
}
