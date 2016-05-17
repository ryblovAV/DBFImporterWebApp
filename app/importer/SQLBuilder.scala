package importer

import importer.reader.source.Field

object SQLBuilder {

  val journalTableName = "cm_import_journal"

  def createFieldBlock(fields: List[Field], f: Field => String, sep: String = ",") =
    fields.foldLeft("")((str, field) => s"$str${f(field)}${if (field.index != fields.length - 1) sep else ""}")

  def generateInsert(tableName: String, fields: List[Field]) = {
    s"""
       |insert into $tableName
       |(${createFieldBlock(fields, field => field.name)})
       |values
       |(${createFieldBlock(fields, (f: Field) => s":${f.name}")})
     """.stripMargin
  }

  def generateTruncate(table: String) = s"truncate table $table"

  def generateCheckExistsTable =
    s"select t.table_name from user_tables t where t.table_name = upper(?)"

  def generateSqlCreateTable(tableName: String, fields: List[Field]) = {

    def getType(fieldType: String) = fieldType match {
      case "78" => "NUMBER(30,10)"
      case "68" => "DATE"
      case _ => "varchar2(500)"
    }

    s"""
       |create table $tableName
       |(
       |${createFieldBlock(fields, f => s"${f.name} ${getType(f.typeField)}", ",\n")}
       |)
     """.stripMargin
  }

  def generateCreateJournal =
    s"""
       |create table $journalTableName
       |(
       |  table_name  varchar2(100),
       |  file_name   varchar2(500),
       |  dt_start    date default sysdate,
       |  dt_end      date,
       |  cnt         number,
       |  cnt_all     number,
       |  dt          date,
       |  message     varchar2(4000)
       |)
     """.stripMargin

  def generatePKJournal =
    s"alter table $journalTableName add constraint PK_$journalTableName primary key (table_name)"

  def generateProgressCopy =
    s"""
       |merge into $journalTableName j
       |using (select ? as table_name, ? as message from dual) j2
       |on (j.table_name = j2.table_name)
       |when matched then
       | update set j.dt = sysdate, j.cnt = 0, j.message = j2.message
       |when not matched then
       | insert
       |  (j.table_name, j.dt, j.message)
       | values
       |  (j2.table_name, sysdate, j2.message)
       """.stripMargin

  def generateProgressJournal(tableName: String, cnt: Integer, message: String) =
    s"""
       |merge into $journalTableName j
       |using (select lower('$tableName') as table_name, $cnt as cnt, '$message' as message from dual) j2
       |on (j.table_name = j2.table_name)
       |when matched then
       | update set j.cnt = j2.cnt, j.dt = sysdate, j.message = j2.message
       |when not matched then
       | insert
       |  (j.table_name, j.cnt, j.dt, j.message)
       | values
       |  (j2.table_name, j2.cnt, sysdate, j2.message)
       """.stripMargin

  def generateMessageJournal(tableName: String, message: String) =
    s"""
       |merge into $journalTableName j
       |using (select lower('$tableName') as table_name, '$message' as message from dual) j2
       |on (j.table_name = j2.table_name)
       |when matched then
       | update set j.dt = sysdate, j.message = j2.message
       |when not matched then
       | insert
       |  (j.table_name, j.dt, j.message)
       | values
       |  (j2.table_name, sysdate, j2.message)
       """.stripMargin

  def generateStartJournal(tableName: String, fileName: String, count: Int) =
    s"""
       |merge into $journalTableName j
       |using (select lower('$tableName') as table_name,  lower('$fileName') as file_name, $count as countAll from dual) j2
       |on (j.table_name = j2.table_name)
       |when matched then
       | update set j.dt_start = sysdate, j.file_name = j2.file_name, j.dt_end = null, j.message = 'start import to DB', j.cnt_all = j2.countAll
       |when not matched then
       | insert
       |  (j.table_name, j.file_name, j.dt_start, j.message, j.cnt_all)
       | values
       |  (j2.table_name, j2.file_name, sysdate, 'start import to DB', j2.countAll)
       """.stripMargin

  def generateEndJournal(tableName: String) =
    s"""
       |merge into $journalTableName j
       |using (select lower('$tableName') as table_name from dual) j2
       |on (j.table_name = j2.table_name)
       |when matched then
       | update set j.dt_end = sysdate, j.message = 'complete'
       |when not matched then
       | insert
       |  (j.table_name, j.dt_end, j.message)
       | values
       |  (j2.table_name, sysdate, 'complete')
       """.stripMargin

  def house48Sql(tableName: String) =
    Seq(
      "delete from lcmccb.cm_dic_addr_house",
    s"""
       |  insert into lcmccb.cm_dic_addr_house
       |    (aoguid,
       |     buildnum,
       |     enddate,
       |     eststatus,
       |     houseguid,
       |     houseid,
       |     housenum,
       |     statstatus,
       |     ifnsfl,
       |     ifnsul,
       |     okato,
       |     oktmo,
       |     postalcode,
       |     startdate,
       |     strucnum,
       |     strstatus,
       |     terrifnsfl,
       |     terrifnsul,
       |     updatedate,
       |     normdoc,
       |     counter)
       |    select aoguid,
       |           buildnum,
       |           enddate,
       |           eststatus,
       |           houseguid,
       |           houseid,
       |           housenum,
       |           statstatus,
       |           ifnsfl,
       |           ifnsul,
       |           okato,
       |           oktmo,
       |           postalcode,
       |           startdate,
       |           strucnum,
       |           strstatus,
       |           terrifnsfl,
       |           terrifnsul,
       |           updatedate,
       |           normdoc,
       |           counter
       |      from $tableName
    """.stripMargin
    )

  def addrObjSql(tableName: String) =
    Seq(
      "delete from lcmccb.cm_dic_addr",
      s"""
         |insert into lcmccb.cm_dic_addr
         |    (actstatus,
         |     aoguid,
         |     aoid,
         |     aolevel,
         |     areacode,
         |     autocode,
         |     centstatus,
         |     citycode,
         |     code,
         |     currstatus,
         |     enddate,
         |     formalname,
         |     ifnsfl,
         |     ifnsul,
         |     nextid,
         |     offname,
         |     okato,
         |     oktmo,
         |     operstatus,
         |     parentguid,
         |     placecode,
         |     plaincode,
         |     postalcode,
         |     previd,
         |     regioncode,
         |     shortname,
         |     startdate,
         |     streetcode,
         |     terrifnsfl,
         |     terrifnsul,
         |     updatedate,
         |     ctarcode,
         |     extrcode,
         |     sextcode,
         |     livestatus,
         |     normdoc)
         |    select actstatus,
         |           aoguid,
         |           aoid,
         |           aolevel,
         |           areacode,
         |           autocode,
         |           centstatus,
         |           citycode,
         |           code,
         |           currstatus,
         |           enddate,
         |           formalname,
         |           ifnsfl,
         |           ifnsul,
         |           nextid,
         |           offname,
         |           okato,
         |           oktmo,
         |           operstatus,
         |           parentguid,
         |           placecode,
         |           plaincode,
         |           postalcode,
         |           previd,
         |           regioncode,
         |           shortname,
         |           startdate,
         |           streetcode,
         |           terrifnsfl,
         |           terrifnsul,
         |           updatedate,
         |           ctarcode,
         |           extrcode,
         |           sextcode,
         |           livestatus,
         |           normdoc
         |      from $tableName
         |      where actstatus=1
         |        and livestatus = 1
    """.stripMargin
    )

  def generateTransferDataSql(fileName: String, tableName: String) = fileName.toUpperCase match {
    case "ADDROBJ.DBF" => addrObjSql(tableName)
    case "HOUSE48.DBF" => house48Sql(tableName)
    case _ => throw new Exception(s"unknown filename: $fileName")
  }


}
