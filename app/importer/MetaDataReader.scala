package importer

import com.linuxense.javadbf.DBFReader

case class Field(name:String,typeField:String, index:Int, nullable:String)

object MetaDataReader {

  def getFields(dbfReader: DBFReader): List[Field] = {

    def createField(dbfReader: DBFReader, index: Int) = {
      val dbfField = dbfReader.getField(index)
      Field(dbfField.getName, dbfField.getDataType.toString, index, "Y")
    }

    (0 until dbfReader.getFieldCount).map((i) => createField(dbfReader, i)).toList
  }

}
