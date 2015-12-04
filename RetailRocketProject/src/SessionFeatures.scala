/**
 * Created by Administrator on 12/2/2015.
 */

class SessionFeatures(val totalNum: Int, avgClicks: Double, totalTime: Long, maxDuraion: Long,
                       avgDuration: Double, maxItemClicks: Int,
                       avgProb: Double, maxProb: Double, oneProb: Double, maxConsecNum: Int, maxConsecDur: Double) {
  override def toString(): String = (totalNum, avgClicks, totalTime, maxDuraion, avgDuration, maxItemClicks, avgProb,
    maxProb, oneProb, maxConsecNum, maxConsecDur).productIterator.mkString(" ")
}
