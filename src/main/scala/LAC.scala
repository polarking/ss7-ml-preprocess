/**
  * Created by kristoffer on 25.04.2016.
  */
object LAC {
  val lacDistanceMap = Map[String, Int](
    "8138-8138" -> 9,
    "8138-8161" -> 12,
    "8138-8189" -> 7,
    "8161-8161" -> 12,
    "8161-8189" -> 8,
    "8161-9321" -> 14,
    "8189-9321" -> 9,
    "9321-9343" -> 15,
    "9343-9385" -> 3,
    "6593-8138" -> 281,
    "6593-8161" -> 337,
    "6593-8189" -> 521,
    "6593-9321" -> 287,
    "6593-9343" -> 137,
    "6593-9385" -> 198,
    "6593-6593" -> 127
  )

  def getDistance(lacToFrom: String): Int = {
    val toFrom = lacToFrom.split("-")
    if(toFrom(0) == toFrom(1)) 10
    else if(!lacDistanceMap.contains(lacToFrom)) 10
    else lacDistanceMap(lacToFrom)
  }

  def lacDecode(lac: String): Int = {
    val split = lac.split(":")
    val lacDec = split(0) + split(1)
    Integer.parseInt(lacDec, 16)
  }
}
