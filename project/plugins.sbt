libraryDependencies ++= Seq(
  "org.apache.avro" % "avro-compiler" % "1.5.1",
  "org.slf4j" % "slf4j-simple" % "1.6.1"
)

resolvers += Classpaths.typesafeResolver

addSbtPlugin("com.typesafe.sbtscalariform" % "sbtscalariform" % "0.3.0")
