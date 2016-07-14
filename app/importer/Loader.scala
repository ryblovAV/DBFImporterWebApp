package importer

import com.linuxense.javadbf.DBFReader
import importer.db.{DBLogWriter, DBWorker, DBWriter, PreDBWriter}
import importer.reader.source.{DBFSourceReader, InputStreamObject}
import play.api.Logger._
import play.api.db.Database

object Loader {

  def createDBFReader(inputStreamObject: InputStreamObject): DBFReader = {
    val reader = new DBFReader(inputStreamObject.inputStream)
    reader.setCharactersetName("Cp866")
    reader
  }

  def load(db: Database, inputStreamObject: InputStreamObject, tableName: String) = {

    info("start loadData")

    val dbfSourceReader = DBFSourceReader(createDBFReader(inputStreamObject))

    val fields = dbfSourceReader.getFields

    info("start pre db writer")

    val logWriter = new DBLogWriter(db, tableName, dbfSourceReader.getRecordCount)
    logWriter.logStart(inputStreamObject.name)

    (new PreDBWriter(db)).preLoadImportTable(tableName, fields)

    info("start run loading")
    StreamWorker.runStream(
      dbfSourceReader,
      () => new DBWriter(db),
      logWriter,
      DBWorker.makeInsertFuture(
        SQLBuilder.generateInsert(tableName, fields),
        tableName
      )
    )
  }


}
