package otus.akka.basics

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior

object  into_actors{

  //1. functional.
  object behaviour_factory_method{
    object Echo {
      def apply(): Behavior[String] = Behaviors.setup{ ctx =>
        Behaviors.receiveMessage{
          case msg =>
            ctx.log.info(msg)
            Behaviors.same
        }
      }
    }
  }


  //2. OOP
  object abstract_behaviour {
    class Echo(ctx: ActorContext[String]) extends AbstractBehavior[String](ctx) {
      override def onMessage(msg: String): Behavior[String] = {
        ctx.log.info(msg)
        this
      }
    }

    object Echo{
      def apply(): Behavior[String] = Behaviors.setup{ctx =>
        new Echo(ctx)
      }
    }

  }
}
