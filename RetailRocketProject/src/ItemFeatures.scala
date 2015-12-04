/**
 * Created by Administrator on 11/28/2015.
 */
class ItemFeatures(val id_item: Int, val indicator_first: Int, val indicator_last: Int, val clicks_num: Int,
val time_spent: Long, val probability: Double, val consecutive_clicks: Int, val max_duration: Long) {

  /*override def equals(other: Any) = other match {
    case that: ItemFeatures => this._id_session == that._id_session && this._id_item == that._id_item
    case _ => false
  }

  override def hashCode: Int =
    41 * (41 + _id_session) + _id_item*/

  //def id_session = _id_session
 // def id_item = _id_item

}
