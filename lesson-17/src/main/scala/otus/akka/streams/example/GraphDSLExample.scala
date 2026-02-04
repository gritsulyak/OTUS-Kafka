package otus.org.apache.pekko.streams.example

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{ClosedShape, Graph}
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, Zip}

import scala.concurrent.{ExecutionContextExecutor, Future}

object GraphDSLExample extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val graph: Graph[ClosedShape.type, Future[Done]] =
    GraphDSL.createGraph(Sink.foreach(println)){ implicit builder => sink =>
    import GraphDSL.Implicits._

    val broadcast = builder.add(Broadcast[Int](2))
    val source: Source[Int, NotUsed] = Source(1 to 100000)

    source ~> broadcast

    val flow: Flow[Int, Int, NotUsed] = Flow.fromFunction(identity[Int])
    val squareFlow: Flow[Int, Int, NotUsed] = Flow.fromFunction(Math.pow(_, 2).toInt)

    val zip = builder.add(Zip[Int, Int])

    broadcast.out(0) ~> flow ~> zip.in0
    broadcast.out(1) ~> squareFlow ~> zip.in1

    // source -> broadcast -> flow ->        zip       -> Sink Foreach println
    //                     \-> squareFlow  /^ (why order not changed
    zip.out ~> sink
    ClosedShape
  }

  var done = RunnableGraph.fromGraph(graph).run()
  done.onComplete(_ => system.terminate())
}
