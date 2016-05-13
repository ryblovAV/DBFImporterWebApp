package importer.db

import play.api.Logger._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

object DBWorker {

  def makeInsertFuture(sqlInsert: String,
                       tableName: String)
                      (dbWriter: DBWriter,
                       l :List[Array[Object]]):Future[Int] = {
    Future {
      blocking {
        info("start load to db")
        dbWriter.loadToDB(l.asJava, sqlInsert, tableName)
        l.length
      }
    }
  }

}
