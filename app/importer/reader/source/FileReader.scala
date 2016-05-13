package importer.reader.source

import java.io.{File => JFile, _}

import jcifs.smb.SmbFile

trait InputStreamObject {
  def name:String
  def inputStream:InputStream
}

object InputStreamObject {
  def apply(jFile: JFile): InputStreamObject = new InputStreamObject() {
    override def name: String = jFile.getName
    override def inputStream: InputStream = new FileInputStream(jFile)
  }
}

trait SourceFolder {
  def listFiles:Array[SourceFile]
}

object SourceFolder {

  def filterDbf(fileName: String) = fileName.takeRight(4).toLowerCase == ".dbf"

  def apply(jFile: JFile): SourceFolder = new SourceFolder {
    override def listFiles: Array[SourceFile] =
      if (jFile.isDirectory) jFile.listFiles().filter(f => filterDbf(f.getName)).map(f => SourceFile(f.getName,f.length,FileUtl.calcCount(f)))
      else Array.empty[SourceFile]
  }

  //SmbUtl.calcCount(f)
  def apply(smbFile: SmbFile):SourceFolder = new SourceFolder {
    override def listFiles: Array[SourceFile] = {
      smbFile.listFiles
        .filter(f => filterDbf(f.getName))
        .filter(f => ((f.getName.toLowerCase.startsWith("house48")) || (f.getName.toLowerCase.startsWith("addrobj"))))
        .sortBy(_.getName)
        .map(f => SourceFile(f.getName,f.length,0))
    }
  }
}

case class SourceFile(name: String, size:Long, count: Int)

object FileReader {

  def getSourceFolder(path: String):SourceFolder = {
    if (SmbUtl.checkIsSmb(path))  SourceFolder(SmbUtl.getSmbFile(path))
    else SourceFolder(new JFile(path))
  }

  def calcCount(path: String, fileName: String) = {
    if (SmbUtl.checkIsSmb(path)) SmbUtl.calcCount(SmbUtl.getSmbFile(path = s"$path$fileName"))
    else FileUtl.calcCount(new JFile(s"$path/$fileName"))
  }

  def getInputStreamObject(path:String, fileName:String, logCopyProgress: Long => Unit): InputStreamObject = {
    if (SmbUtl.checkIsSmb(path)) SmbUtl.getStremObject(path = path,fileName = fileName,logCopyProgress = logCopyProgress)
    else FileUtl.getStreamObject(path = path,fileName = fileName)
  }

}
