package edu.fuberlin

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  type Cellid = (Int, Int, Int)

  val MIN_LONGITUDE = -74.3
  val MAX_LONGITUDE = -73.25

  val MIN_LATITUDE = 40.5
  val MAX_LATITUDE = 40.9

  /**
    * Turn the 3-tuple for cell id into a single Int.
    * The grid size must be above 0.001 degrees for the function to remain its bijective property.
    * By that the amount of data send over network is reduced drastically.
    * @param cellid Each component of the id must not have a range larger than 1000
    * @return
    */
  def compose(cellid:Cellid):Int =  (cellid._1 << 19) | (cellid._2 << 9) | cellid._3

  /**
    * Oppsite of [[compose()]]
    * @param cellid
    * @return
    */
  def decompose(cellid:Int):Cellid = (cellid >> 19, (cellid >> 9) & 0x1FF, cellid & 0x1FF)
}