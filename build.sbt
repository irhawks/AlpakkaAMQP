name := "AlpakkaAMQP"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
   "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % "0.16",
   "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.16",
   "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.16"
)
