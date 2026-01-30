package akka_akka_streams.Akka

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SpawnProtocol}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.Props
import akka_akka_streams.Akka.into_actors.behaviour_factory_method

import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.concurrent.duration._


object AkkaMain{
  def main(args: Array[String]): Unit ={
    val system = ActorSystem[String](behaviour_factory_method.Echo(), "Echo")
    system ! "Hello"

    Thread.sleep(3000)
    system.terminate()
  }
}


object AkkaMain2{
  object Supervisor{
    def apply(): Behavior[SpawnProtocol.Command] = Behaviors.setup{ ctx =>
      ctx.log.info(ctx.self.toString)
      SpawnProtocol()
    }
  }

  def main(args: Array[String]): Unit ={
    implicit val system = ActorSystem[SpawnProtocol.Command](Supervisor(), "root")
    implicit  val ec = system.executionContext
    implicit val timeout = Timeout(3 seconds)

    val echo: Future[ActorRef[String]] = system.ask(
      SpawnProtocol.Spawn(behaviour_factory_method.Echo(), "Echo", Props.empty, _))

    for (ref <- echo)
      ref ! "Hello from ask"
  }
}