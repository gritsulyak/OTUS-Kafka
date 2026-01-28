package otus.org.apache.pekko.streams.example

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import java.time.Instant
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

object BackpressureExample extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val source: Source[Int, NotUsed] = Source(1 to 10)
  private val flow: Flow[Int, Int, NotUsed] = Flow.fromFunction(identity[Int])
    .throttle(1, 1.seconds)

  source.via(flow)
    .runWith(Sink.foreach(v => println(Instant.now(), v)))
    .onComplete(_ => system.terminate())
}
