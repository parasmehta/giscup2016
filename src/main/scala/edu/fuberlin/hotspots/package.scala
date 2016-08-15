package edu.fuberlin

import scala.util.matching.Regex

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  type Cellid = (Int, Int, Int)

  //TODO Find a way to adjust this function to different cell sizes
  /**
    * Turn the 3-tuple for cell id into a single Int.
    * By that the amount of data send over network is reduced drastically.
    * @param c Each component of the id must not have a range larger than 1000
    * @return
    */
  def compose(c:Cellid):Int = (c._1 + 74300) * 1000000 + (c._2 - 40500) * 1000 + c._3

  /**
    * Oppsite of  [[compose()]]
    * @param k
    * @return
    */
  def decompose(k:Int):Cellid = ((k / 1000000) - 74300, ((k % 1000000) / 1000 + 40500), k % 1000)
}