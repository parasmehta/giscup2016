package edu.fuberlin

import scala.util.matching.Regex

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  type Cellid = (Int, Int, Int)

  def compose(c:Cellid):Int = (c._1 + 74300) * 1000000 + (c._2 - 40500) * 1000 + c._3
  def decompose(k:Int):Cellid = ((k / 1000000) - 74300, ((k % 1000000) / 1000 + 40500), k % 1000)
}