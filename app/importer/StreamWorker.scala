package importer

import com.linuxense.javadbf.DBFReader
import importer.db.{DBLogWriter, DBWriter}
import play.api.Logger._

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration.Duration

object StreamWorker {

  val partitionCount = 5000
  val threadCount = 5

  def getRecords(reader: DBFReader): Stream[List[Array[Object]]] = {

    def take(reader: DBFReader, records: List[Array[Object]] = Nil, cnt: Int = 0): List[Array[Object]] = {
      if (cnt == partitionCount) records
      else {
        val record = reader.nextRecord
        if (record == null) records
        else take(reader, record :: records, cnt + 1)
      }
    }

    take(reader) #:: getRecords(reader)
  }

  def makeStream(dbfReader: DBFReader): Stream[List[Array[Object]]] =
    getRecords(dbfReader).takeWhile(_.size != 0)

  @tailrec
  def makeListFuture(i: Int = 0,
                     lf: List[Future[Int]] = List.empty[Future[Int]],
                     dbWriters: Map[Int, DBWriter],
                     futureBuilder: (DBWriter, List[Array[Object]]) => Future[Int],
                     s: Stream[List[Array[Object]]]): (List[Future[Int]], Stream[List[Array[Object]]]) = {
    if ((s.isEmpty) || (i == threadCount))
      (lf, s)
    else
      makeListFuture(i + 1, futureBuilder(dbWriters(i), s.head) :: lf, dbWriters, futureBuilder, s.tail)
  }

  def makeCounterStream(reader: DBFReader):Stream[Int] =
    (if (reader.nextRecord() != null) 1 else 0) #:: makeCounterStream(reader)

  def runOneStream(count: Int = 0,
                   s: Stream[List[Array[Object]]],
                   dbWriters: Map[Int, DBWriter],
                   dbfLogWriter: DBLogWriter,
                   futureBuilder: (DBWriter, List[Array[Object]]) => Future[Int]
                  ): Stream[Int] = {

    def calcCount(lf: List[Future[Int]]) = lf.map(Await.result(_, Duration.Inf)).foldLeft(0)((b, i) => b + i)

    if (s.isEmpty) {
      Stream.empty[Int]
    }
    else {
      makeListFuture(dbWriters = dbWriters, futureBuilder = futureBuilder, s = s) match {
        case (lf, s) => {
          val nextCount = count + calcCount(lf)
          info(s"-------------- count = $nextCount ----------------")
          dbfLogWriter.logProgress(nextCount)
          nextCount #:: runOneStream(nextCount, s, dbWriters, dbfLogWriter, futureBuilder)
        }
      }
    }
  }

  def runStream(dbfReader: DBFReader,
                dbWriterBuilder: () => DBWriter,
                dbfLogWriter: DBLogWriter,
                futureBuilder: (DBWriter, List[Array[Object]]) => Future[Int]) = {

    def makeDbWriters(dbWriterBuilder: () => DBWriter)
      = (0 until threadCount).map(i => (i, dbWriterBuilder())).toMap

    runOneStream(
      s = makeStream(dbfReader),
      dbWriters = makeDbWriters(dbWriterBuilder),
      dbfLogWriter = dbfLogWriter,
      futureBuilder = futureBuilder)
  }

}
