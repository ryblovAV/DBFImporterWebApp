package importer.db

import importer.SQLBuilder
import org.springframework.jdbc.core.JdbcTemplate
import play.api.db.Database

class DBLogWriter(db: Database, tableName: String, countAll: Int = 0) {

  def logMessage(message: String) = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)
        jdbcTemplate.update(
          SQLBuilder.generateMessageJournal(
            tableName,
            message
          )
        )
    }
  }

  def logStart(fileName: String) = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)

        (new PreDBWriter(db)).preLoadJournal

        jdbcTemplate.update(
          SQLBuilder.generateStartJournal(tableName,fileName,countAll)
        )
    }
  }

  def logProgress(count: Int) = {

    def calcProgress(count: Int) = {
      Math.round(100.0 * count / countAll)
    }

    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)

        jdbcTemplate.update(
          SQLBuilder.generateProgressJournal(
            tableName,
            count,
            s"import to DB ${calcProgress(count)}%"
          )
        )
    }
  }

  def logCopyProgress(progress: Long):Unit = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)
        jdbcTemplate.update(SQLBuilder.generateProgressCopy,tableName.toLowerCase, s"copy file $progress%")
    }
  }

  def logEnd = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)

        jdbcTemplate.execute(
          SQLBuilder.generateEndJournal(tableName))
    }
  }

}
