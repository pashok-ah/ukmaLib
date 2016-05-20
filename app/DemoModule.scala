import com.google.inject.{AbstractModule, TypeLiteral}
import net.codingwell.scalaguice.ScalaModule
import securesocial.core.RuntimeEnvironment
import services.secsocial.MyEnvironment


class DemoModule extends AbstractModule with ScalaModule {
  override def configure() {
    val environment: MyEnvironment = new MyEnvironment
    bind(new TypeLiteral[RuntimeEnvironment] {}).toInstance(environment)
  }
}
