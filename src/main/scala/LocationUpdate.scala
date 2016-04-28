/**
  * Created by kristoffer on 25.04.2016.
  */
class LocationUpdate(val timeEpoch: Double, val byteLength: Double, val travelDist: Double, val lastUpdate: Double, val prevLac: Int) {
  override def toString: String = timeEpoch + "," + byteLength + "," + travelDist + "," + lastUpdate + "," + prevLac
}

object LocationUpdate {
  def apply(timeEpoch: Double, byteLength: Double, travelDist: Double, lastUpdate: Double, prevLac: Int): LocationUpdate =
    new LocationUpdate(timeEpoch, byteLength, travelDist, lastUpdate, prevLac)
  def apply(): LocationUpdate = new LocationUpdate(0,0,0,0, 0)
}
