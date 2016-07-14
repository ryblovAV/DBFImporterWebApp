import TestActorApp.ImportExcellActor.StartImportExcelMessage
import TestActorApp.WriteToDbActor.WriteToDBMessage
import akka.actor.{Actor, ActorSystem, Props}

object TestActorApp extends App {

  class ExcelReadEngine {
    def start(path:String,onRead: List[Array[Object]] => Unit) = {
      for (i <- 1 to 100) {
        Thread.sleep(1000)
        val records = (1 to 100).map(i => List(i.toString).toArray[Object]).toList
        onRead(records)
      }
    }
  }

  object ImportExcellActor {
    case class StartImportExcelMessage(path: String)
    def props = Props[ImportExcellActor]
  }

  class ImportExcellActor extends Actor {

    def onReadHandler(records: List[Array[Object]]):Unit = {
      context.actorOf(WriteToDbActor.props) ! WriteToDBMessage(records)
    }

    override def receive: Receive = {
      case StartImportExcelMessage(path) => (new ExcelReadEngine).start(path,onReadHandler)
    }
  }



  object WriteToDbActor {
    case class WriteToDBMessage(records: List[Array[Object]])
    def props = Props[WriteToDbActor]
  }

  class WriteToDbActor extends Actor {
    override def receive: Receive = {
      case WriteToDBMessage(records) => println(s"write to db ${records.length}")
    }
  }

  val path: String = "//Users//user//data//svod//copy//copy.xlsx"

  val system = ActorSystem("ImportSystem")
  val excelImportActor = system.actorOf(ImportExcellActor.props,"importExcel")
  excelImportActor ! StartImportExcelMessage(path)

}
