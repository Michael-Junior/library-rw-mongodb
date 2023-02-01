ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "library-rw-mongodb"
  )

val mongoVersion = "4.8.1"
val logback = "1.4.5"
val logging = "3.9.5"

libraryDependencies ++= Seq(
  ("org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion).cross(CrossVersion.for3Use2_13),
  "ch.qos.logback" % "logback-classic" % logback,
  "com.typesafe.scala-logging" %% "scala-logging" % logging,
)