package importer

import com.linuxense.javadbf.DBFReader
import importer.db.{DBLogWriter, DBWorker, DBWriter, PreDBWriter}
import importer.reader.source.InputStreamObject
import play.api.Logger._
import play.api.db.Database

object Loader {

  def createDBFReader(inputStreamObject: InputStreamObject): DBFReader = {
    val reader = new DBFReader(inputStreamObject.inputStream)
    reader.setCharactersetName("Cp866")
    reader
  }

  def load(db: Database, inputStreamObject: InputStreamObject, tableName: String) = {

    val dbfReader = createDBFReader(inputStreamObject)

    val fields = MetaDataReader.getFields(dbfReader)

    info("start pre db writer")

    val logWriter = new DBLogWriter(db, tableName, dbfReader.getRecordCount)
    logWriter.logStart(inputStreamObject.name)

    (new PreDBWriter(db)).preLoadImportTable(tableName, fields)

    info("start run loading")
    StreamWorker.runStream(
      dbfReader,
      () => new DBWriter(db),
      logWriter,
      DBWorker.makeInsertFuture(
        SQLBuilder.generateInsert(tableName, fields),
        tableName
      )
    )
  }


}
