package otus.akka.basics

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorSystem, Behavior, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object MutableState extends App{
  sealed trait  Command

  case class Deposite(v:Int) extends Command
  case class Withdraw(v:Int) extends Command
  case class Get() extends Command
  case class Stop() extends Command

  object Account{

    def apply(am: Int): Behavior[Command] = Behaviors.setup{ctx =>
      var amount: Int = am

      Behaviors.receiveMessage{
        case Stop() => // instead PoisonPill for untyped
          Behaviors.stopped
        case Deposite(v) =>
          amount = amount + v
          ctx.log.info(s"Deposite $v to amount $amount. Total state is $amount")
          Behaviors.same
        case Withdraw(v) =>
          amount = amount - v
          ctx.log.info(s"Withdrow $v from amount $amount. Total state is $amount")
          Behaviors.same
        case Get() =>
          ctx.log.info(s"Total state is $amount")
          Behaviors.same
      }
    }
  }

  def apply():Behavior[NotUsed] =
    Behaviors.setup{ctx =>

      val account1 = ctx.spawn(Account(2000), "actor_1")
      val account2 = ctx.spawn(Account(42), "actor_2")

      ctx.watch(account1)
      ctx.watch(account2)

      account1 ! Get()
      account2 ! Get()

      account1 ! Deposite(1)
      account2 ! Get()
      account1 ! Deposite(1)
      account1 ! Withdraw(2002)

      for (_ <- 1 to 1000000) {
        account1 ! Withdraw(1)
      }
      account2 ! Stop()
      account1 ! Stop() // Сообщение встанет в конец очереди после всех Withdraw

      // Используем переменную для отслеживания количества живых акторов
      var stoppedChildren = 0
      def waitTermination(count: Int): Behavior[NotUsed] = Behaviors.receiveSignal {
        case (context, Terminated(ref)) =>
          val remaining = count - 1
          if (remaining == 0)
            Behaviors.stopped
          else
            waitTermination(remaining)
      }
      waitTermination(2)
    }

  implicit val system = ActorSystem(MutableState(), "akka_typed")

  // Await.ready(system.whenTerminated, Duration.Inf)

}