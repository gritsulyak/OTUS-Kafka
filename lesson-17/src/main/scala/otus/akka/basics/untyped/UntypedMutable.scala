package otus.akka.basics.untyped

import org.apache.pekko.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props, Terminated}

// Сообщения
case class Deposite(v: Int)
case class Withdraw(v: Int)
case class Get()
case object Sync // Сообщение для синхронизации
case object Ack  // Подтверждение

class Account(initialAmount: Int) extends Actor with ActorLogging {
  var amount: Int = initialAmount

  def receive: Receive = {
    case Deposite(v) => amount += v
    case Withdraw(v) => amount -= v
    case Get()       => log.info(s"Total state is $amount")
    case Sync        => sender() ! Ack // Отвечаем, когда дошли до этой точки в очереди
  }
}

class MainActor extends Actor with ActorLogging {
  val account1 = context.actorOf(Props(new Account(2_000_000)), "actor_1")
  val account2 = context.actorOf(Props(new Account(42)), "actor_2")

  context.watch(account1)
  context.watch(account2)

  var stoppedChildren = 0

  // 1. Отправляем миллион сообщений
  log.info("Sending 1,000,000 messages...")
  for (_ <- 1 to 1000000) { account1 ! Withdraw(1) }

  // 2. Вместо PoisonPill отправляем Sync
  // Это сообщение встанет в очередь ПОСЛЕ всех Withdraw
  account2 ! Get()
  account1 ! Get()
  account2 ! Sync
  account1 ! Sync

  def receive: Receive = {
    case Ack =>
      // Когда пришел Ack, мы уверены, что актор обработал всё, что было до Sync
      log.info(s"Received Ack from ${sender().path.name}, now killing it...")
      sender() ! PoisonPill

    case Terminated(ref) =>
      stoppedChildren += 1
      if (stoppedChildren == 2) {
        log.info("All children dead. Terminating system.")
        context.system.terminate()
      }
  }
}


object MutableStateClassic extends App {
  val system = ActorSystem("classic_system")
  system.actorOf(Props[MainActor], "main")
}