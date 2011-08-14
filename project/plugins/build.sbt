sbtPlugin := true

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
    "org.apache.avro" % "avro-compiler" % "1.5.1",
    "org.slf4j" % "slf4j-simple" % "1.6.1",
    "com.typesafe.sbt-scalariform" %% "sbt-scalariform" % "0.1.2"
  )
