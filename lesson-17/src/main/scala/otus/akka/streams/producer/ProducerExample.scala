package otus.org.apache.pekko.streams.producer

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.Producer
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

object ProducerExample extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val config = system.settings.config.getConfig("kafka.producer")
  private val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
  private val topic = system.settings.config.getConfig("kafka").getString("topic")

  Source(1 to 1000)
    .throttle(1, 1.seconds)
    .map(_.toString)
    .log(name = "producer")
    .addAttributes(
      Attributes.logLevels(onElement = Attributes.LogLevels.Info),
    )
    .map(value => new ProducerRecord[String, String](topic, value))
    .runWith(Producer.plainSink(producerSettings))
}
