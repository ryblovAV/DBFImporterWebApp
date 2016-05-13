package importer.db

import importer.{Field, SQLBuilder}
import org.springframework.jdbc.core.JdbcTemplate
import play.api.Logger._
import play.api.db.Database

class PreDBWriter(db: Database) {

  def preLoadJournal = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)

        if (jdbcTemplate.queryForList(
          SQLBuilder.generateCheckExistsTable, SQLBuilder.journalTableName).size == 0) {
          jdbcTemplate.execute(SQLBuilder.generateCreateJournal)
          jdbcTemplate.execute(SQLBuilder.generatePKJournal)
        }
    }
  }

  def preLoadImportTable(tableName: String, fields: List[Field]) = {
    db.withConnection{
      conn =>
          val jdbcTemplate = new JdbcTemplate(db.dataSource)

          if (jdbcTemplate.queryForList(
            SQLBuilder.generateCheckExistsTable, tableName).size == 0) {

              jdbcTemplate.execute(SQLBuilder.generateSqlCreateTable(tableName, fields))
              info(s"create table $tableName")
          } else {
              jdbcTemplate.execute(SQLBuilder.generateTruncate(tableName))
              info(s"truncate table $tableName")
          }
    }
  }
}


