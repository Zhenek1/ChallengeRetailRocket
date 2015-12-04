/**
 * Created by Administrator on 12/2/2015.
 */
import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer
object FileSort {
  var filePath = ""
  var i = 0
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  def mergeSort(f: File, min_lines_num: Double): File = {
    filePath = f.getAbsolutePath.substring(0, f.getAbsolutePath.lastIndexOf(File.separator)) + File.separator
    val n = countLines(f) / 2
    if (n <= min_lines_num) sortFile(f)
    else {
      val (f1, f2) = divideFile(f, n)
      merge(mergeSort(f1, min_lines_num), mergeSort(f2, min_lines_num))
    }
  }

  def merge(f1: File, f2: File) : File = {
    val r1 = new BufferedReader(new FileReader(f1))
    val r2 = new BufferedReader(new FileReader(f2))
    val merged_file = new File(filePath + "m" + i)
    i += 1
    val bw = new BufferedWriter(new FileWriter(merged_file))
    var switcher = 0
    var l1 = r1.readLine
    var l2 = r2.readLine
    var session_id = -1

    def compareDates = {
      val t1 = LocalDateTime.parse(l1.split(",")(1), formatter)
      val t2 = LocalDateTime.parse(l2.split(",")(1), formatter)
      if (t1.isBefore(t2)) {
        bw.write(l1 + "\n")
        session_id = l1.split(",")(0).toInt
        switcher = 1
        l1 = r1.readLine
      }
      else {
        bw.write(l2 + "\n")
        session_id = l2.split(",")(0).toInt
        switcher = 2
        l2 = r2.readLine
      }
    }
    def chooseLine1 = {
      if (l1.split(",")(0).toInt == session_id) {
        bw.write(l1 + "\n")
        l1 = r1.readLine
      } else {
        compareDates
      }
    }
    def chooseLine2 = {
      if (l2.split(",")(0).toInt == session_id) {
        bw.write(l2 + "\n")
        l2 = r2.readLine
      } else {
        compareDates
      }
    }

    while (l1 != null && l2 != null) {
      switcher match {
        case 0 => compareDates
        case 1 => chooseLine1
        case 2 => chooseLine2
      }
    }
    if (l1 == null) {
      while (l2 != null) {
        bw.write(l2 + "\n")
        l2 = r2.readLine()
      }
    } else {
      while (l1 != null) {
        bw.write(l1 + "\n")
        l1 = r1.readLine()
      }
    }
    r1.close()
    r2.close()
    bw.close()
    return merged_file
  }

  def countLines(f: File): Int = {
    val reader = new BufferedReader(new FileReader(f))
    var lines = 0
    while (reader.readLine() != null) lines += 1
    reader.close()
    return lines
  }

  def sortFile(f: File) : File = {
    val tempFile = new File(filePath + "tempFile.dat")
    val reader = new BufferedReader(new FileReader(f))
    val bw = new BufferedWriter(new FileWriter(tempFile))
    val lines = ListBuffer[String]()
    var line:String = ""
    while ({line = reader.readLine() ; line != null}) {
      lines += line
    }
    val lines_grouped = lines.groupBy(_.split(",")(0))
    def sortFunc(str1: (String, ListBuffer[String]), str2: (String, ListBuffer[String])) : Boolean = {
      val t1 = LocalDateTime.parse(str1._2(0).split(",")(1), formatter)
      val t2 = LocalDateTime.parse(str2._2(0).split(",")(1), formatter)
      t1.isBefore(t2)
    }
    val lines_sorted = lines_grouped.toList.sortWith(sortFunc)
    for (lineBuf <- lines_sorted)
      for (line <- lineBuf._2)
        bw.write(line + "\n")
    reader.close()
    bw.close()
    f.delete()
    tempFile.renameTo(f)
    tempFile.toString
    return new File(filePath + f.getName)
  }
  def divideFile(f: File, n: Int) : (File, File) = {
    val f1 = new File(filePath + f.getName + "p1")
    val f2 = new File(filePath + f.getName + "p2")
    val reader = new BufferedReader(new FileReader(f))
    val bw1 = new BufferedWriter(new FileWriter(f1))
    val bw2 = new BufferedWriter(new FileWriter(f2))
    var session_id = 0
    var line: String = ""
    for (i <- 1 to n) {
      line = reader.readLine()
      bw1.write(line + "\n")
      session_id = line.split(",")(0).toInt
    }
    while ({line = reader.readLine(); line != null && line.split(",")(0).toInt == session_id}) {
      bw1.write(line + "\n")
    }
    if (line != null)  bw2.write(line + "\n")
    while ( {
      line = reader.readLine(); line != null
    }) {
      bw2.write(line + "\n")
    }
    reader.close()
    bw1.close()
    bw2.close()
    return (f1, f2)
  }
}
