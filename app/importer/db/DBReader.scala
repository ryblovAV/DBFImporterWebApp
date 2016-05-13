package importer.db


import java.sql.ResultSet

import importer.SQLBuilder
import org.springframework.jdbc.core.{JdbcTemplate, RowMapper}
import play.api.db.Database

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class ProgressInfo(tableName: String,
                        fileName: String,
                        dtStart: String,
                        dtEnd: String,
                        cnt: Int,
                        cntAll: Int,
                        dt: String,
                        message: String
                       )

case class StartImportAttr(fileName: String, tableName: String)

object DBReader {


  val sql =
    s"""
       |select table_name,
       |       file_name,
       |       to_char(dt_start,'DD.MM.YYYY HH24:MI:SS') as dt_start,
       |       to_char(dt_end,'DD.MM.YYYY HH24:MI:SS') as dt_end,
       |       cnt,
       |       cnt_all,
       |       to_char(dt,'DD.MM.YYYY HH24:MI:SS') as dt,
       |       message
       |  from cm_import_journal
     """.stripMargin


  val mapper = new RowMapper[ProgressInfo] {
    override def mapRow(rs: ResultSet, rowNum: Int): ProgressInfo = {
      ProgressInfo(
        tableName = rs.getString(1),
        fileName = rs.getString(2),
        dtStart = rs.getString(3),
        dtEnd = rs.getString(4),
        cnt = rs.getInt(5),
        cntAll = rs.getInt(6),
        dt = rs.getString(7),
        message = rs.getString(8)
      )
    }
  }

  def readProgress(db: Database): Map[String, ProgressInfo] = {
    db.withConnection {
      conn =>
        val jdbcTemplate = new JdbcTemplate(db.dataSource)

        val existsJournalTable = jdbcTemplate.queryForList(SQLBuilder.generateCheckExistsTable, SQLBuilder.journalTableName).size != 0

        if (existsJournalTable)
          jdbcTemplate.query(sql, mapper).asScala.toList.groupBy(_.fileName).mapValues(l => l.head)
        else
          Map.empty[String,ProgressInfo]
    }
  }

}
