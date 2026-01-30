package otus.akka.basics
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, SpawnProtocol}
import org.apache.pekko.util.Timeout

import scala.concurrent.ExecutionContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.Props
import otus.akka.basics.Dispatcher.JsonParser.{Parse, ParseResponse}
import otus.akka.basics.Dispatcher.LogWorker.LogResponse
import otus.akka.basics.Dispatcher.TaskDispatcher.{LogWork, ParseUrl}
import otus.akka.basics.into_actors.behaviour_factory_method

import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.concurrent.duration._
import java.util.UUID

object Dispatcher extends App {
  object  TaskDispatcher{
    sealed trait CommandDispatcher

    case class ParseUrl(url: String) extends CommandDispatcher
    case class LogWork(url: String) extends CommandDispatcher
    case class LogResponseWrapper(msg: LogResponse) extends CommandDispatcher
    case class ParseResponseWrapper(msg: ParseResponse) extends CommandDispatcher

    def apply(): Behavior[CommandDispatcher] = Behaviors.setup{ctx =>
      val logAdapter  = ctx.messageAdapter[LogResponse](rs => LogResponseWrapper(rs))
      val parseAdapter = ctx.messageAdapter[ParseResponse](rs => ParseResponseWrapper(rs))

      Behaviors.receiveMessage{
        case LogWork(work) =>
          val logWorker = ctx.spawn(LogWorker(), s"LogWorker${UUID.randomUUID()}")
          ctx.log.info(s"Dispatcher got log ${work}")
          logWorker ! LogWorker.Log(work, logAdapter)
          Behaviors.same

        case ParseUrl(url) =>
          val urlParse = ctx.spawn(JsonParser(), s"JsonParser${UUID.randomUUID()}")
          ctx.log.info(s"Dispatcher got url $url")
          urlParse ! Parse(url, parseAdapter)
          Behaviors.same

        case LogResponseWrapper(m) =>
          ctx.log.info("Log done")
          Behaviors.same
        case ParseResponseWrapper(m) =>
          ctx.log.info("Parse done")
          Behaviors.same

      }

    }


  }

  object LogWorker {
    sealed  trait LogRequest

    case class Log(l: String, replyTo: ActorRef[LogResponse]) extends LogRequest

    sealed trait LogResponse

    case class LogDone() extends LogResponse

    def apply(): Behavior[LogRequest] = Behaviors.setup{ctx =>
      Behaviors.receiveMessage{
        case Log(l, replyTo) =>
          ctx.log.info("log work in progress")
          replyTo! LogDone()
          Behaviors.stopped
      }
    }
  }

  object JsonParser {
    sealed trait  ParseCommand

    case class Parse(json: String, replyTo: ActorRef[ParseResponse]) extends ParseCommand

    sealed trait ParseResponse
    case class ParseDone() extends  ParseResponse

    def apply(): Behavior[ParseCommand] = Behaviors.setup{ ctx =>
      Behaviors.receiveMessage{
        case Parse(json, replyTo) =>
          ctx.log.info("json parsing in progress")
          replyTo ! ParseDone()
          Behaviors.stopped
      }
    }

  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup{ctx =>
      val dispatcherActorRef = ctx.spawn(TaskDispatcher(), "disp")

      dispatcherActorRef ! LogWork("bla bla bla")
      dispatcherActorRef ! ParseUrl("url url url")

      Behaviors.same
    }

  implicit val system = ActorSystem(Dispatcher(), "disp_actor_system")

  Thread.sleep(3000)
  system.terminate()



}
