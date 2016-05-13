package controllers

import akka.stream.scaladsl.Source
import com.google.inject.Inject
import importer.Loader
import importer.db._
import importer.reader.source.{FileReader, SmbUtl, SourceFile}
import play.api.Logger._
import play.api.db.Database
import play.api.libs.functional.syntax._
import play.api.libs.json.Json._
import play.api.libs.json._
import play.api.mvc._
import play.twirl.api.Html

class Application @Inject() (db: Database) extends Controller {

//  val path = "//Users//user//data//import//1"
  val path = "smb://192.168.231.213/stage/ФИАС/"
//    val path = "ftp://prodapp1.lesk.ru"

  var activeTables = Set.empty[String]

  implicit val rds: Reads[StartImportAttr] = (
      (__ \ 'fileName).read[String] and
      (__ \ 'tableName).read[String]
    )(StartImportAttr.apply _)

  implicit val progressWriters = new Writes[ProgressInfo] {
    def writes(progress: ProgressInfo) = Json.obj(
      "table_name" -> progress.tableName,
      "dt_start" -> progress.dtStart,
      "dt_end" -> progress.dtEnd,
      "count" -> progress.cnt,
      "cntAll" -> progress.cntAll,
      "dt" -> progress.dt,
      "message" -> progress.message
    )
  }

  implicit val filesWriters = new Writes[SourceFile] {
    def writes(sourceFile: SourceFile) = Json.obj(
      "name" -> sourceFile.name.toLowerCase,
      "size" -> Math.round(sourceFile.size/1000000.0),
      "currentCount" -> 0,
      "count" -> sourceFile.count
    )
  }

  def index() = Action {
    Ok(views.html.listFiles())
  }

  def startImport = Action(parse.json) {

    def makeSource(fileName: String, tableName: String, logger: DBLogWriter): Source[Int,_] = {


      logger.logStart(fileName)

      Source(
        Loader.load(
          db,
          FileReader.getInputStreamObject(
            path = path,
            fileName = fileName,
            logCopyProgress = logger.logCopyProgress),
          tableName
        ).force)

    }

    request => {
      request.body.validate[StartImportAttr].map {
        case StartImportAttr(fileName,tableName) =>
          if (!activeTables.contains(tableName)) {
            val dbfLogWriter = new DBLogWriter(db = db, tableName = tableName, countAll = FileReader.calcCount(path = path, fileName = fileName))
            try {
              info(s"start -> name = $fileName, tableName = $tableName")

              activeTables = activeTables + tableName

              makeSource(fileName, tableName, dbfLogWriter)

              dbfLogWriter.logMessage("start transfer data")
              PostDBWriter.transferData(db = db, fileName = fileName, tableName = tableName)

              SmbUtl.removeFile(fileName = fileName)

              dbfLogWriter.logEnd

            } catch {
                case e:Exception =>
                  error(s"error!!!: $e")
                  dbfLogWriter.logMessage(s"Error!!! ${e.getMessage}")
            } finally {
              activeTables = activeTables - tableName
            }
          } else {
            info(s"sorry, already start -> name = $fileName, tableName = $tableName")
          }
          Ok
      }.recoverTotal {
        e =>
          BadRequest(Html("<p>" + e + "</p>"))
      }
    }
  }

  def listFiles = Action {
    val l = FileReader.getSourceFolder(path).listFiles
    val json = toJson(l)
    Ok(json)
  }

  def progress = Action {
    Ok(toJson(DBReader.readProgress(db)))
  }

}