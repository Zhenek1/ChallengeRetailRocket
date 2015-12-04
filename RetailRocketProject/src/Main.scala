
import java.time.LocalDateTime
import java.io._
import java.time.format.DateTimeFormatter
import java.time._

import scala.collection.mutable.ListBuffer
import scala.io.Source;
import scala.collection.mutable

object Main {
  val file3 = new File("D:\\Downloads\\yoochoose-dataFull\\session_features3.dat")
  val file2 = new File("D:\\Downloads\\yoochoose-dataFull\\session_features2.dat")
  val file = new File("D:\\Downloads\\yoochoose-dataFull\\session_features.dat")
  val bw = new BufferedWriter(new FileWriter(file))
  val buys_file = "D:\\Downloads\\yoochoose-dataFull\\yoochoose-buys.dat"
  val clicks_file = "D:\\Downloads\\yoochoose-dataFull\\yoochoose-clicks.dat"
  var clicks_file_sorted = new File("D:\\Downloads\\yoochoose-dataFull\\clicks_sorted.dat")
  var clicks_file_part = "D:\\Downloads\\yoochoose-dataFull\\yoochoose-clicks_split\\yoochoose-clicks_part1.dat"
  val testSort_file = new File("D:\\Downloads\\yoochoose-dataFull\\testSort\\clicks_part1.dat")
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  def main(args: Array[String]) {

  /*
  MergeSort using files. Uncomment if file is not sorted by dates
   */

  // clicks_file_sorted = FileSort.mergeSort(testSort_file, 2 * math.pow(10, 6))
   // println("Sorted file: " + file.getName)
    val buys_map = mutable.HashMap[Int, List[BuyEvent]]()
    val clicks_map = mutable.LinkedHashMap[Int, List[ClickEvent]]()
    val events_list = ListBuffer[ClickEvent]()
    val buyEvents = ListBuffer[BuyEvent]()
    var current_id = -1

    //Get buys data
    for (line <- Source.fromFile(buys_file).getLines) {
      val values = line.split(",")
      if (current_id == -1) {
        current_id = values(0).toInt
      } else
      if (current_id != values(0).toInt)
      {
        buys_map += (current_id -> buyEvents.toList)
        buyEvents.clear()
        current_id = values(0).toInt
      }
      buyEvents += new BuyEvent(LocalDateTime.parse(values(1), formatter), values(2).toInt, values(3).toInt, values(4).toInt)
    }
    buys_map += (current_id -> buyEvents.toList)
    buyEvents.clear()


    current_id = -1
    val r = new BufferedReader(new FileReader(clicks_file_sorted))
    var currLine = r.readLine
    val values = currLine.split(",")
    current_id = values(0).toInt
    var startEvent = new ClickEvent(LocalDateTime.parse(values(1), formatter), values(2).toInt, values(3))
    events_list += startEvent
    while (currLine != null) {
      //Read sorted file and create features
      val values = currLine.split(",")
      if (current_id != values(0).toInt) {
        clicks_map += (current_id -> events_list.toList)
        //Measure difference between dates (day + surrounding days)
        if (daysDifference(startEvent.timestamp, LocalDateTime.parse(values(1), formatter)) > 1) {
          outputFeatures(bw, clicks_map, buys_map, current_id, startEvent)
          startEvent = clicks_map.find(x => daysDifference(startEvent.timestamp, x._2(0).timestamp) >= 1).get._2(0)
        }
        events_list.clear()
        current_id = values(0).toInt
      }
      events_list += new ClickEvent(LocalDateTime.parse(values(1), formatter), values(2).toInt, values(3))
      currLine = r.readLine()
    }
    clicks_map += (current_id -> events_list.toList)
    outputFeatures(bw, clicks_map, buys_map, current_id, startEvent)
    startEvent = clicks_map.find(x => daysDifference(startEvent.timestamp, x._2(0).timestamp) >= 1).get._2(0)
    outputFeatures(bw, clicks_map, buys_map, current_id, startEvent)
    r.close()
    bw.close()
    events_list.clear()
    clicks_map.clear()
    println("Features calculated!")
  }


  def daysDifference(localDateTime1: LocalDateTime, localDateTime2: LocalDateTime) : Int = {
    Duration.between(localDateTime1.withHour(0).withMinute(0).withSecond(0).withNano(0),
      localDateTime2.withHour(0).withMinute(0).withSecond(0).withNano(0)).toDays.toInt
    }

  def outputFeatures(bw: BufferedWriter, clicks_map: mutable.LinkedHashMap[Int, List[ClickEvent]],
                     buys_map: mutable.HashMap[Int, List[BuyEvent]], current_id: Int, startEvent: ClickEvent) = {
    val filtered_map = clicks_map.filter(x => daysDifference(startEvent.timestamp, x._2(0).timestamp) == 0)
    val itemFeatures_list = calculateItemFeatures(filtered_map, clicks_map)
    val session_features = calculateSessionFeatures(filtered_map, itemFeatures_list)
    for ((session_id, session_feature) <- session_features) {
      val output = {if (buys_map.contains(session_id)) 1 else 0}
      bw.write(output + "," + session_feature.toString() + "\n")
    }
    clicks_map.retain((k, x) => daysDifference(startEvent.timestamp, x(0).timestamp) >= 0)
  }

  def calculateItemFeatures(filtered_map: mutable.LinkedHashMap[Int, List[ClickEvent]],
                            clicks_map: mutable.LinkedHashMap[Int, List[ClickEvent]]): mutable.LinkedHashMap[Int, List[ItemFeatures]] = {
    val itemFeatures_map = mutable.LinkedHashMap[Int, List[ItemFeatures]]()
    val clickEventsList_distinct = clicks_map.map(x => (x._2.groupBy(_.id_item).map(_._2.head))).flatten
    val clickEventsList_distinct_counter = clickEventsList_distinct.groupBy(_.id_item).mapValues(_.size)
    for ((session_id, events_list) <- filtered_map) {
      val itemFeatures_list = ListBuffer[ItemFeatures]()
      val clickEvents_distinct = events_list.groupBy(_.id_item).map(_._2.head)
      val consecMap = consecClicks2(events_list)
      clickEvents_distinct.foreach { event =>
        //Item features calculations (f1-f7 respectively)
        val firstLast = firstLastIndicator(events_list, event)
        val clicksNum = clicksNumber(events_list, event)
        val timeSp = timeSpent(events_list, event)
        val consec2 = consecClicksNumber(consecMap, event)
        val consecTime = consecClicksTime(consecMap, event)
        val prob = probability(clickEventsList_distinct_counter, event, clicks_map.size, 25)
        itemFeatures_list += new ItemFeatures(event.id_item, firstLast._1, firstLast._2, clicksNum, timeSp, prob, consec2, consecTime)
      }
      itemFeatures_map += (session_id -> itemFeatures_list.toList)
    }
    return itemFeatures_map
  }

  def calculateSessionFeatures(filtered_map: mutable.LinkedHashMap[Int, List[ClickEvent]],
                               itemFeatures_map: mutable.LinkedHashMap[Int, List[ItemFeatures]]) : mutable.LinkedHashMap[Int, SessionFeatures] = {
    val sessionFeatures_map = mutable.LinkedHashMap[Int, SessionFeatures]()
    for ((session_id, events_list) <- filtered_map) {
      //Session features calculations (p1-p11 respectively)
      val itemFeatures_list = itemFeatures_map(session_id)
      val totalNum = events_list.size
      val avgClicks = totalNum.toDouble / itemFeatures_list.size
      val totalTime: Long = Duration.between(events_list(0).timestamp, events_list.last.timestamp).toMillis
      val (maxDuraion, avgDuration) = maxAvgDuration(events_list)
      val maxItemClicks = events_list.groupBy(_.id_item).map(_._2.size).max
      val avgProb = itemFeatures_list.map(_.probability).sum / itemFeatures_list.size
      val maxProb = itemFeatures_list.map(_.probability).max
      val oneProb = 1 - itemFeatures_list.map(1 - _.probability).product
      val maxConsecNum = itemFeatures_list.map(_.consecutive_clicks).max
      val maxConsecDur = itemFeatures_list.map(_.max_duration).max
      sessionFeatures_map += (session_id -> new SessionFeatures(totalNum, avgClicks, totalTime, maxDuraion,
        avgDuration, maxItemClicks, avgProb, maxProb, oneProb, maxConsecNum, maxConsecDur))
    }
    return sessionFeatures_map
  }


  def probability(clickEventsList_distinct_counter: Map[Int, Int], clickEvent: ClickEvent, total_sessions: Int, threshold: Int) : Double = {
    val sessions_num = clickEventsList_distinct_counter(clickEvent.id_item)
    if (sessions_num >= threshold)
      return sessions_num.toDouble / total_sessions
    else
      return 0.0
  }

  def consecClicks(clicksBuffer: mutable.ArrayBuffer[ClickEvent], clickEvent: ClickEvent) : Int =  {
    return clicksBuffer.foldLeft(0, 0) { (total, event) =>
      event.id_item match {
        case clickEvent.id_item =>
          if (total._2 + 1 > total._1) (total._1 + 1, total._2 + 1)
          else (total._1, total._2 + 1)
        case _ => (total._1, 0)
      }
    }._1
  }

  def consecClicksNumber(consecMap: mutable.Map[Int, List[ClickEvent]], clickEvent: ClickEvent) : Int = {
    consecMap(clickEvent.id_item).size
  }

  def consecClicksTime(consecMap: mutable.Map[Int, List[ClickEvent]], clickEvent: ClickEvent) : Long = {
    val consecEvents = consecMap(clickEvent.id_item)
    var maxTime : Long = 0
    for (idx <- 0 until consecEvents.size - 1) {
      val duration = Duration.between(consecEvents(idx).timestamp, consecEvents(idx + 1).timestamp).toMillis
      if (duration > maxTime)
        maxTime = duration
    }
    return maxTime
  }

  def consecClicks2(clicksBuffer: List[ClickEvent]) : mutable.Map[Int, List[ClickEvent]] = {
    val map = mutable.Map[Int, List[ClickEvent]]()
    var cur_buff = clicksBuffer.toList
    while (!cur_buff.isEmpty) {
      cur_buff match {
        case Nil => Nil
        case h :: t =>
          val segment = cur_buff.takeWhile(h.id_item == _.id_item)
          if (map.getOrElseUpdate(segment(0).id_item, segment).size < segment.size)
            map(segment(0).id_item) = segment
          cur_buff = cur_buff.drop(segment.length)
      }
    }
    return map
  }

  def firstLastIndicator(clicksBuffer: List[ClickEvent], clickEvent: ClickEvent) : (Int, Int) = {
    var firstItem = 0
    var lastItem = 0
    if (clicksBuffer(0).id_item == clickEvent.id_item)
      firstItem = 1
    if (clicksBuffer.last.id_item == clickEvent.id_item)
      lastItem = 1
    return (firstItem, lastItem)
  }

  def clicksNumber(clicksBuffer: List[ClickEvent], clickEvent: ClickEvent) : Int = {
    return clicksBuffer.foldLeft(0) { (total, event) =>
      event.id_item match {
        case clickEvent.id_item => total + 1
        case _ => total
      }
    }
  }

  def timeSpent(clicksBuffer: List[ClickEvent], clickEvent: ClickEvent): Long = {
    return clicksBuffer.indices.foldLeft[Long](0){ (total, idx) =>
      if (idx == clicksBuffer.size - 1)
        return total
      else
        clicksBuffer(idx).id_item match {
          case clickEvent.id_item =>
            total + Duration.between(clicksBuffer(idx).timestamp, clicksBuffer(idx + 1).timestamp).toMillis
          case _=> total
        }
    }
  }

  def maxAvgDuration(clicksBuffer: List[ClickEvent]) : (Long, Double) = {
    if (clicksBuffer.size == 1)
      return (0, 0.0)
    return clicksBuffer.indices.foldLeft[(Long, Double)](0, 0){ (counter, idx) =>
      if (idx == clicksBuffer.size - 1)
        (counter._1, counter._2.toDouble / idx)
      else {
        val duration = Duration.between(clicksBuffer(idx).timestamp, clicksBuffer(idx + 1).timestamp).toMillis
        if (duration > counter._1)
          (duration, counter._2 + duration)
        else
          (counter._1, counter._2 + duration)
      }
    }
  }
}
