package output_storage

import scala.reflect.ClassTag

/**
  * Created by hungdv on 17/03/2017.
  */
/**
  * Base class for all storage output from DStream.
  * ps
  *
  * @tparam T
  */
abstract class StorageWriter[T: ClassTag] extends Serializable{
  def persistToStorage(
                  storageConfig: Map[String,String]
                  ): Unit
}
