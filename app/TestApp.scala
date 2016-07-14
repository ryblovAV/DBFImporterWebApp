import java.io.{File, InputStream}

import org.apache.poi.openxml4j.opc.{OPCPackage, PackageAccess}
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.util.SAXHelper
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler
import org.apache.poi.xssf.eventusermodel.{ReadOnlySharedStringsTable, XSSFReader, XSSFSheetXMLHandler}
import org.apache.poi.xssf.usermodel.XSSFComment
import org.xml.sax.InputSource

import play.Logger._

object TestApp extends App {

  private val path: String = "//Users//user//data//svod//copy//copy.xlsx"

  class XLSX2CSV(opcPackage: OPCPackage) {

    case class Page(name: String, stream: InputStream)

    class SheetToCSV extends SheetContentsHandler {

      var countRecord = 0

      override def startRow(rowNum: Int): Unit = {
        countRecord += 1
      }

      override def endRow(rowNum: Int): Unit = {
          info(s"count: $countRecord")
      }

      override def cell(cellReference: String, formattedValue: String, comment: XSSFComment): Unit = {
//        info(s"cell: $formattedValue")
      }

      override def headerFooter(text: String, isHeader: Boolean, tagName: String): Unit = ()
    }


    val stringsTable = new ReadOnlySharedStringsTable(opcPackage)
    val xssfReader = new XSSFReader(opcPackage)
    val styles = xssfReader.getStylesTable

    def sheets(iter: XSSFReader.SheetIterator):Stream[Page] = {
      if (iter.hasNext) {
        val stream = iter.next()
        Page(iter.getSheetName,stream) #:: sheets(iter)
      } else Stream.empty
    }

    def processSheet(page: Page) = {

      val formatter = new DataFormatter()
      val sheetSource = new InputSource(page.stream)

      val sheetParser = SAXHelper.newXMLReader()
      val handler = new XSSFSheetXMLHandler(styles,null,stringsTable,new SheetToCSV(),formatter,false)
      sheetParser.setContentHandler(handler)
      sheetParser.parse(sheetSource)
    }

    def process() = {

      val iter = xssfReader.getSheetsData.asInstanceOf[XSSFReader.SheetIterator]

      sheets(iter).foreach(p => processSheet(p))
    }


  }

  def test = {
    val file = new File(path)

    val p = OPCPackage.open(file.getPath, PackageAccess.READ)
    val xlsx2csv = new XLSX2CSV(p)

    xlsx2csv.process()
  }

  test
}
