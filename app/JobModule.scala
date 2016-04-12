import com.google.inject.AbstractModule
import play.api.libs.concurrent.AkkaGuiceSupport
import services.scheduler.Scheduler
import services.scheduler.SchedulerActor

/**
  * Created by P. Akhmedzianov on 09.04.2016.
  */
class JobModule extends AbstractModule with AkkaGuiceSupport {
  def configure() = {
    bindActor[SchedulerActor]("scheduler-actor")
    bind(classOf[Scheduler]).asEagerSingleton()
  }
}