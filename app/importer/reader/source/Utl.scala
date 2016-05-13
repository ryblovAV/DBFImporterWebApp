package importer.reader.source

import play.Logger._

import java.io.{FileInputStream, FileOutputStream, File => JFile}

import com.linuxense.javadbf.DBFReader
import jcifs.smb.{NtlmPasswordAuthentication, SmbFile, SmbFileInputStream}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FileUtl {
  def calcCount(file: JFile):Int = {
    val inputStream = new FileInputStream(file)
    val count = (new DBFReader(inputStream)).getRecordCount
    inputStream.close()
    count
  }

  def getStreamObject(path: String, fileName: String) =
    InputStreamObject(new JFile(s"$path/$fileName"))

}

object SmbUtl {

  val copyPath = "data"

  def removeFile(fileName: String) = {
    val path = s"$copyPath/$fileName"
    val jFile = new JFile(path)
    val res: Boolean = jFile.delete
    info(s"try remove file $path. ${if (res) "Success" else "Failure"}")
  }

  def getStremObject(path: String, fileName: String, logCopyProgress: Long => Unit) =
    InputStreamObject(createCopy(path,fileName,logCopyProgress))

  def calcCount(file: SmbFile):Int = {
    val inputStream = new SmbFileInputStream(file)
    val count = (new DBFReader(inputStream)).getRecordCount
    inputStream.close()
    count
  }


  def createCopy(path: String, fileName: String, logCopyProgress: Long => Unit) = {

    val downloadFile = new JFile(s"$copyPath/$fileName")

    val fileOutputStream = new FileOutputStream(downloadFile)
    val remoteFile: SmbFile = getSmbFile(path = s"$path$fileName")
    val fileInputStream = remoteFile.getInputStream()
    val buf = new Array[Byte](16 * 1024 * 1024)

    def calcProgress(remoteLengt: Long)(downloadLength: Long)= {
      Math.round(100.0 * downloadLength/remoteLengt)
    }

    var success = false
    Future {
      while (!success) {
        logCopyProgress(calcProgress(remoteFile.length)(downloadFile.length()))
        Thread.sleep(5000)
      }
    }

    try {
      var len = fileInputStream.read(buf)
      while (len > 0) {
        fileOutputStream.write(buf, 0, len)
        len = fileInputStream.read(buf)
      }
    } finally {
      success = true
      fileInputStream.close();
      fileOutputStream.close()
    }

    downloadFile
  }

  def getSmbFile(path: String) = {
    val user = "test:test"
    val auth = new NtlmPasswordAuthentication(user)
    new SmbFile(path, auth)
  }

  def checkIsSmb(server: String) = server.take(6) == "smb://"
}


