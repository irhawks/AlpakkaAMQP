package com.abhi

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl._
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import akka.stream.scaladsl._

/**
 * Created by ASrivastava on 10/2/17.
 */
object AlpakkaAMQPPublisher extends App {

  implicit val actorSystem = ActorSystem()
  implicit val actorMaterializer = ActorMaterializer()

  val queueDeclaration = QueueDeclaration("queue", durable = true)

  //val exchangeName = "amqp-conn-it-spec-pub-sub-" + System.currentTimeMillis()
  val exchangeName = "amqp-nb"
  val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")

  val uri = "amqp://172.17.0.1:5672/"

  val connectionProvider =
    AmqpDetailsConnectionProvider(List(("172.17.0.1", 5672)))
      .withHostsAndPorts(("172.17.0.1", 5672))

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(connectionProvider)
          .withRoutingKey("")
          .withExchange(exchangeName)
          .withDeclarations(exchangeDeclaration)
        )

      val resource = getClass.getResource("/countrycapital.csv")
      val path = Paths.get(resource.toURI)
      val source = FileIO.fromPath(path)

      val graph = RunnableGraph.fromGraph(GraphDSL.create(amqpSink){implicit builder =>
        s =>
          import GraphDSL.Implicits._
          source ~> s.in
          ClosedShape
      })

      val future = graph.run()

      future.onComplete { _ =>
        actorSystem.terminate()
      }
      Await.result(actorSystem.whenTerminated, Duration.Inf)
}
