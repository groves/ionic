import sbt._
import Keys._

object Ionic extends Build {
  lazy val ionic = Project (
    "ionic", file ("."), settings = Defaults.defaultSettings ++ Seq(
        organization := "com.bungleton",
        version := "0.1-SNAPSHOT",
        name := "ionic",
        scalaVersion := "2.9.0-1",

        resolvers += "Local Maven Repository" at Path.userHome.asURL + "/.m2/repository",

        libraryDependencies ++= Seq(
          "com.google.guava" % "guava" % "r09",
          "org.apache.avro" % "avro" % "1.5.1",
          "org.jboss.netty" % "netty" % "3.2.4.Final",
          "com.threerings" % "fisy" % "1.0-SNAPSHOT",
          "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test",
          "org.testng" % "testng" % "6.1" % "test"
        )
    ))
}
