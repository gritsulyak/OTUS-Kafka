package otus.org.apache.pekko.streams.example

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.FileIO

import java.nio.file.Paths
import scala.concurrent.{ExecutionContextExecutor, Future}

object FileExample extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val resultF: Future[IOResult] = FileIO.fromPath(Paths.get("src/main/resources/input.txt"))
    .to(FileIO.toPath(Paths.get("src/main/resources/output.txt")))
    .run()

  resultF.onComplete(ioResult => {
    println(ioResult)
    system.terminate()
  })
}
