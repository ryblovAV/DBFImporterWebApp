package importer.db

import importer.SQLBuilder
import org.springframework.jdbc.core.JdbcTemplate
import play.api.db.Database

import play.Logger._

object PostDBWriter {

  def transferData(db: Database, fileName: String, tableName: String) = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)
        SQLBuilder.generateTransferDataSql(fileName = fileName, tableName = tableName)
          .foreach(sql => jdbcTemplate.execute(sql))

        info("transfer data complete")
    }
  }


}
