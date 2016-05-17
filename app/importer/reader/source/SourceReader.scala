package importer.reader.source

import java.io.FileInputStream
import java.util

import com.linuxense.javadbf.DBFReader
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Cell, Row}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.annotation.tailrec

case class Field(name: String, typeField: String, index: Int, nullable: String)


trait SourceReader {
  def nextRecord: Array[Object]

  def getFields: List[Field]

  def getRecordCount: Int
}

case class DBFSourceReader(dbfReader: DBFReader) extends SourceReader {
  override def nextRecord: Array[Object] = dbfReader.nextRecord()

  override def getFields: List[Field] = {
    def createField(dbfReader: DBFReader, index: Int) = {
      val dbfField = dbfReader.getField(index)
      Field(dbfField.getName, dbfField.getDataType.toString, index, "Y")
    }

    (0 until dbfReader.getFieldCount).map((i) => createField(dbfReader, i)).toList
  }

  override def getRecordCount: Int = dbfReader.getRecordCount
}

case class ExcelSourceReader(path: String) extends SourceReader {

  val CELL_TYPE_NUMBER = 0
  val CELL_TYPE_STRING = 1

  case class ExcelField(name: String, cellType: Int)

  private def getCellsAttr[T](cellIterator: util.Iterator[Cell], getAttr: Cell => T): Stream[T] = {
    if (cellIterator.hasNext) getAttr(cellIterator.next()) #:: getCellsAttr(cellIterator, getAttr)
    else Stream.empty[T]
  }

  @tailrec
  private def calcCount(rowIterator: util.Iterator[Row], count:Int = 0):Int = {
    if (rowIterator.hasNext) {
      val row = rowIterator.next()
      val cell = row.getCell(0)
      println("type = " + cell.getCellType)

      calcCount(rowIterator,count+1)
    } else
      count
  }

  val wb = new XSSFWorkbook(new FileInputStream(path))
  val sheet = wb.getSheetAt(0)

  override def getRecordCount: Int = calcCount(sheet.rowIterator())

  val excelFields = {
    val rowIterator: util.Iterator[Row] = sheet.rowIterator

    if (rowIterator.hasNext) {
      val titleRow = rowIterator.next()
      val titles = getCellsAttr(cellIterator = titleRow.cellIterator, getAttr = _.getStringCellValue).toList

      if (rowIterator.hasNext) {
        val valueRow = rowIterator.next()
        val cellTypes = getCellsAttr(cellIterator = valueRow.cellIterator, getAttr = _.getCellType).toList

        (titles zip cellTypes).map {
          case (title, cellType) => ExcelField(name = title, cellType = cellType)
        }.toArray
      }
      else Array.empty[ExcelField]
    }
    else Array.empty[ExcelField]
  }

  val rowIterator = sheet.rowIterator

  override def nextRecord: Array[Object] = {

    def getCellValue(cell: Cell, dataType: Int):Object = dataType match {
      case CELL_TYPE_NUMBER => cell.getNumericCellValue.asInstanceOf[AnyRef]
      case CELL_TYPE_STRING => cell.getStringCellValue
    }

    if (rowIterator.hasNext) {
      val row = rowIterator.next
      getCellsAttr(cellIterator = row.cellIterator,getAttr = cell => getCellValue(cell, excelFields(cell.getColumnIndex).cellType)).toArray
    } else null

  }


  override def getFields: List[Field] = {

    def getTypeName(cellType: Int) = cellType match {
      case CELL_TYPE_NUMBER=> "NUMBER"
      case CELL_TYPE_STRING => "VARCHAR2(1000)"
    }

    excelFields.zipWithIndex.map {
      case (field, index) => Field(name = field.name, typeField = getTypeName(field.cellType), index = index, nullable = "Y")
    }.toList
  }


}
