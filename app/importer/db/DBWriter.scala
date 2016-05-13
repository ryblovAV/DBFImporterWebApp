package importer.db

import java.util

import org.springframework.jdbc.core.JdbcTemplate
import play.api.db.Database

class DBWriter(db: Database) {

  def loadToDB(records:util.List[Array[Object]],sqlInsert:String,tableName:String): Unit = {
    db.withConnection {
      conn => {
        val jdbcTemplate = new JdbcTemplate(db.dataSource)
        jdbcTemplate.batchUpdate(sqlInsert, records)
      }
    }
  }
}
