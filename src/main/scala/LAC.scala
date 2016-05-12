/**
  * Provides some helper functions used to handle LACs.
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

  /**
    * Provides a metric for the distance between two LACs.
    * @param lacToFrom The two LACs which will be checked, in the form xxxx-yyyy.
    * @return The distance between two LACs.
    */
  def getDistance(lacToFrom: String): Int = {
    val toFrom = lacToFrom.split("-")
    if(toFrom(0) == toFrom(1))
      if(toFrom(0) == "6593" || toFrom(1) == "6593") 185
      else 10
    else if(!lacDistanceMap.contains(lacToFrom))
      if(toFrom(0) == "6593" || toFrom(1) == "6593") 183
      else 9
    else lacDistanceMap(lacToFrom)
  }

  /**
    * Decodes a hex LAC in the form xx:yy to an integer.
    * @param lac The hex LAC to decode.
    * @return The LAC as an Int.
    */
  def lacDecode(lac: String): Int = {
    val split = lac.split(":")
    val lacDec = split(0) + split(1)
    Integer.parseInt(lacDec, 16)
  }
}
