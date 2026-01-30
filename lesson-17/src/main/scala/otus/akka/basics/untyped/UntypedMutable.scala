package otus.akka.basics.untyped
// import akka.actor. -> import org.apache.pekko.
import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props, Terminated}

// Сообщения остаются такими же
case class Deposite(v: Int)
case class Withdraw(v: Int)
case class Get()

class Account(initialAmount: Int) extends Actor with ActorLogging {
  var amount: Int = initialAmount // Мутабельное состояние внутри класса

  def receive: Receive = {
    case Deposite(v) =>
      amount += v
      log.info(s"Deposite $v. Total: $amount")
    case Withdraw(v) =>
      amount -= v
      log.info(s"Withdraw $v. Total: $amount")
    case Get() =>
      log.info(s"Total state is $amount")
    // В untyped не нужно обрабатывать Stop вручную, для этого есть PoisonPill
  }
}

class MainActor extends Actor {
  val account1 = context.actorOf(Props(new Account(2000)), "actor_1")
  val account2 = context.actorOf(Props(new Account(42)), "actor_2")

  // Следим за завершением (DeathWatch)
  context.watch(account1)
  context.watch(account2)

  var stoppedChildren = 0

  // Логика запуска
  account1 ! Get()
  account2.tell(Get(), self)

  for (_ <- 1 to 1000000) { account1 ! Withdraw(1) }

  // PoisonPill отправляется в очередь как обычное сообщение.
  // Актор обработает его только после всех Withdraw.
  account2 ! PoisonPill
  account1 ! PoisonPill

  def receive: Receive = {
    case Terminated(ref) =>
      stoppedChildren += 1
      if (stoppedChildren == 2) {
        context.system.terminate()
      }
  }
}

object MutableStateClassic extends App {
  val system = ActorSystem("classic_system")
  system.actorOf(Props[MainActor], "main")
}