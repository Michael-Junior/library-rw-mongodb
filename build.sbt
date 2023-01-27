ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "library-rw-mongodb"
  )

val mongoVersion = "4.7.2"

libraryDependencies ++= Seq(
  ("org.mongodb.scala" %% "mongo-scala-driver" % mongoVersion).cross(CrossVersion.for3Use2_13)
)