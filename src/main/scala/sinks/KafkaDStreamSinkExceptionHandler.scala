package sinks
import org.apache.kafka.clients.producer.{Callback,RecordMetadata}

/**
  * Created by hungdv on 10/03/2017.
  */
class KafkaDStreamSinkExceptionHandler extends Callback {
  import java.util.concurrent.atomic.AtomicReference

  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata,exception: Exception): Unit = {
    lastException.set(Option(exception))
  }

  def throwExceptionIfAny(): Unit = lastException.getAndSet(None).foreach(ex => throw ex)
}
