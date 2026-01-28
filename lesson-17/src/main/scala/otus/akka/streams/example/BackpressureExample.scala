package otus.org.apache.pekko.streams.example

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

import java.time.Instant
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt

object BackpressureExample extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  // Генерируем пару (число, метка времени)
  private val source: Source[(Int, Long), NotUsed] = Source(1 to 10)
    .map(v => (v, System.currentTimeMillis()))

  // Обновляем Flow, чтобы он принимал и пробрасывал кортеж
  private val flow: Flow[(Int, Long), (Int, Long), NotUsed] =
    Flow.fromFunction((pair: (Int, Long)) => pair)
      .mapAsync(parallelism = 2) { pair =>
        Future {
          // Имитация работы: здесь происходит параллельная обработка
          println(s"Processing $pair on thread ${Thread.currentThread().getName}")
          pair
        }
      }
      .throttle(1, 1.second)

  source.via(flow)
    .runWith(Sink.foreach { case (v, ts) =>
      println(s"Value: $v, Source Time: ${Instant.ofEpochMilli(ts)}, Current Time: ${Instant.now()}")
    })
    .onComplete(_ => system.terminate())
}
