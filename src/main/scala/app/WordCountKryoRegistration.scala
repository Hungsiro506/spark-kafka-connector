package app
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import com.hungsiro.spark_kafka.core.streaming.KafkaPayloadStringCodec
/**
  * Created by hungdv on 13/03/2017.
  */
class WordCountKryoRegistration extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[KafkaPayloadStringCodec])

    //
    // register avro specific records using twitter chill
    //
    //kryo.register(classOf[MyAvroType], AvroSerializer.SpecificRecordBinarySerializer[MyAvroType])

  }
}
