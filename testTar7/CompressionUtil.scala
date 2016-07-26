package com.ibm.analytics.ngp.pipeline

/**
 * Created by Shally Sangal (shsangal@in.ibm.com)
 */
import java.util
import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils

object CompressionUtil {

  /**
   * Create a new Tar from a root directory
   *
   * @param directory
   * the base directory
   * @param filename
   * the output filename
   * @param absolute
   * store absolute filepath (from directory) or only filename
   * @return True if OK
   */
  def createTarFromDirectory(directory: String, filename: String, absolute: Boolean): Boolean = {
    var taos: TarArchiveOutputStream = null
    try {
      val rootDir: File = new File(directory)
      val saveFile: File = new File(filename)
      taos = new TarArchiveOutputStream(new FileOutputStream(saveFile))
      taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU)
      recurseFiles(rootDir, rootDir, taos, absolute)
    }
    catch {
      case e: FileNotFoundException =>
        return false
      case e2: IOException =>
        try {
          taos == null match {
            case true =>
            case false => taos.close()
          }
        }
        catch {
          case e: IOException =>
        }
        return false
    }

    try {
      taos.finish()
      taos.flush()
      taos.close()
    }
    catch {
      case e1: IOException =>
    }
    true
  }

  /**
   * Recursive traversal to add files
   *
   * @param root
   *             root path
   * @param file
   *             file
   * @param taos
   *             Tar archieve output stream
   * @param absolute
   *                 to include/exclude absolute path
   * @throws IOException
   *                     throws IO Exception
   */
  @throws(classOf[IOException])
  private def recurseFiles(root: File, file: File, taos: TarArchiveOutputStream, absolute: Boolean) {
    if (file.isDirectory) {
      val files: Array[File] = file.listFiles
      for (file2 <- files) {
        recurseFiles(root, file2, taos, absolute)
      }
    }
    else if ((!file.getName.endsWith(".tar")) && (!file.getName.endsWith(".TAR"))) {
      var filename: String = null
      if (absolute) {
        filename = file.getAbsolutePath.substring(root.getAbsolutePath.length)
      }
      else {
        filename = file.getName
      }
      val tae: TarArchiveEntry = new TarArchiveEntry(filename)
      tae.setSize(file.length)
      taos.putArchiveEntry(tae)
      val fis: FileInputStream = new FileInputStream(file)
      IOUtils.copy(fis, taos)
      taos.closeArchiveEntry()
    }
  }

  /**
   *
   * @param path
   *             compress file path
   */
  def gzip(path: String) {
    val buf = new Array[Byte](2048)
    val src: File = new File(path)
    val dst: File = new File(path + ".gz")
    var in: BufferedInputStream = null
    var out: GZIPOutputStream = null
    try {
      in = new BufferedInputStream(new FileInputStream(src))
      out = new GZIPOutputStream(new FileOutputStream(dst))
      var n: Int = in.read(buf)
      while (n >= 0) {
        out.write(buf, 0, n)
        n = in.read(buf)
      }
    }
    catch {
      case fnfEx: FileNotFoundException =>
        System.err.printf("File Not Found: %s", path)
      case ex: Exception =>
        System.err.printf("Permission Denied: %s", path)
    } finally {
      try {
        out == null match {
          case true =>
          case false =>
            out.flush()
            out.close()
        }
        in == null match {
          case true =>
          case false => in.close()
        }
      } catch {
        case ioEx: IOException =>
      }
    }
  }

  /**
   *
   * @param compressedFile
   *                       compressed file path
   * @param decompressedFile
   *                         de compressed file path
   */
  def unGunZip(compressedFile: String, decompressedFile: String) {
    val buf = new Array[Byte](2048)
    var gZIPInputStream: GZIPInputStream = null
    var fileOutputStream: FileOutputStream = null
    try {
      val fileIn: FileInputStream = new FileInputStream(compressedFile)
      gZIPInputStream = new GZIPInputStream(fileIn)
      fileOutputStream = new FileOutputStream(decompressedFile)
      var bytes_read: Int = 0
      while ( {
        bytes_read = gZIPInputStream.read(buf)
        bytes_read
      } > 0) {
        fileOutputStream.write(buf, 0, bytes_read)
      }
      System.out.println("The file was decompressed successfully!")
    }
    catch {
      case ex: IOException =>
        ex.printStackTrace()
    } finally {
      try {
        gZIPInputStream == null match {
          case true =>
          case false => gZIPInputStream.close()
        }
        fileOutputStream == null match {
          case true =>
          case false => fileOutputStream.close()
        }
      } catch {
        case ioEx: IOException =>
      }
    }
  }

  /**
   * Extract all files from Tar into the specified directory
   *
   * @param tarFile
   *                tar file to un tar
   * @param directory
   *                  destination directory
   * @return the list of extracted filenames
   * @throws IOException
   *                     throws IO Exception
   */
  @throws(classOf[IOException])
  @throws(classOf[FileNotFoundException])
  def unTar(tarFile: String, directory: String): java.util.List[String] = {
    val tarFileObj = new File(tarFile)
    val directoryFileObj = new File(directory)

    val result: java.util.List[String] = new util.ArrayList[String]
    var in: TarArchiveInputStream = null
    try {
      val inputStream: InputStream = new FileInputStream(tarFileObj)
      in = new TarArchiveInputStream(inputStream)
      var entry: TarArchiveEntry = in.getNextTarEntry
      while (entry != null) {
        if (entry.isDirectory) {
          entry = in.getNextTarEntry
        } else {
          val curFile: File = new File(directoryFileObj, entry.getName)
          val parent: File = curFile.getParentFile
          if (!parent.exists) {
            parent.mkdirs
          }
          val out: OutputStream = new FileOutputStream(curFile)
          IOUtils.copy(in, out)
          out.close()
          result.add(entry.getName)
          entry = in.getNextTarEntry
        }
      }
    } finally {
      in == null match {
        case true =>
        case false => in.close()
      }
    }
    result
  }

  @throws(classOf[FileNotFoundException])
  @throws(classOf[IOException])
  def delete(path: String): Boolean = {
    var result: Boolean = true
    val file: File = new File(path)
    file.exists() match {
      case false =>
        println(s"Directory: $path does not exist.")
        result = false
      case true =>
        result = delete(file)
    }
    result
  }

  @throws(classOf[IOException])
  def delete(file: File): Boolean = {
    var result: Boolean = true
    file.isDirectory match {
      case true =>
        file.list().isEmpty match {
          case true =>
            file.delete()
            println(s"Directory:${file.getAbsolutePath} is deleted.")
          case false =>
            for (temp: String <- file.list()) {
              delete(new File(file, temp))
            }
            file.list().isEmpty match {
              case true =>
                file.delete()
                println(s"Directory $file is deleted.")
              case false =>
                result = false
            }
        }
      case false =>
        file.delete()
        println(s"File: $file is deleted.")
    }
    result
  }

}
