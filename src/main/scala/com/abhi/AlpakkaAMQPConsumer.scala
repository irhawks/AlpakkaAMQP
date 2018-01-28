package com.abhi

import akka.Done
import akka.dispatch.ExecutionContexts
import akka.util.ByteString

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream._
//import akka.stream.scaladsl.{GraphDSL, Keep, Merge, Sink, Source}
import akka.stream.scaladsl._

//import akka.stream.alpakka.amqp.{AmqpConnectionUri, IncomingMessage, NamedQueueSourceSettings, QueueDeclaration}
import akka.stream.alpakka.amqp.scaladsl._
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.javadsl.AmqpSource

import akka.util.ByteString
import scala.concurrent._
//import scala.concurrent.{Await, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Created by ASrivastava on 10/2/17.
 */

object AlpakkaAMQPConsumer extends App {
  implicit val actorSystem = ActorSystem()
  implicit val actorMaterializer = ActorMaterializer()
  val queueName = "queue"
  val queueDeclaration = QueueDeclaration(queueName, durable = true)

  val connectionProvider =
    AmqpDetailsConnectionProvider(List(("172.17.0.1", 5672)))
      .withHostsAndPorts(("172.17.0.1", 5672))

      val exchangeName = "amqp-nb"
      val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")

      val source = AmqpSource.atMostOnceSource(
        TemporaryQueueSourceSettings(connectionProvider, exchangeName).withDeclarations(exchangeDeclaration),
        bufferSize = 1
      )

      val flow1 = Flow[IncomingMessage].map(msg => msg.bytes)
      val flow2 = Flow[ByteString].map(_.utf8String)
      val sink = Sink.foreach[String](println)
      val graph = RunnableGraph.fromGraph(GraphDSL.create(sink){implicit builder =>
        s =>
          import GraphDSL.Implicits._
          source ~> flow1 ~> flow2 ~> s.in
          ClosedShape
      })
      val future = graph.run()
      future.onComplete{ _ =>
        actorSystem.terminate()
      }
      Await.result(actorSystem.whenTerminated, Duration.Inf)
}
