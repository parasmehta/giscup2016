package edu.fuberlin.hotspots

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
case class SuperCell(val cells:Map[Cellid, Int], val size:Int, val base:Cellid){
  def this(singleCell:(Cellid, Int), size: Int, base:Cellid) = {
    this(immutable.HashMap[Cellid, Int](singleCell), size, base)
  }
}

class SuperCellFactory(val size:Int) extends Serializable {
  private def base(cellID:Cellid):Cellid = {
    val (x, y, t) = cellID
    val xcor = x match {case x if x % size == 0 => x case x => ((x / size) - 1) * size}
    (xcor, (y / size) * size, (t / size) * size)
  }

  private def superID(cellID:Cellid):Cellid = {
    val baseCoords = base(cellID)
    (baseCoords._1 / size, baseCoords._2 / size, baseCoords._3 / size)
  }

  private def offsets(coordinate:Long):Seq[Int] = {
    coordinate % size match {
      case 0 => Seq(-1, 0)
      case mod if mod == size - 1  => Seq(0, 1)
      case _ => Seq(0)
    }
  }

  def create(cell:(Cellid, Int)): Array[(Cellid, SuperCell)] = {
    val mainCell = Seq[(Cellid, SuperCell)]((superID(cell._1), new SuperCell(cell, size, base(cell._1))))
    val (x, y, t) = cell._1
    val (offX, offY, offT) = (offsets((x % size) + size), offsets(y), offsets(t))
    val buffer:ListBuffer[(Cellid, SuperCell)] = new ListBuffer[(Cellid, SuperCell)]()
    for(oX <- offX; oY <- offY; oT <- offT){
      val pseudoCellID = (x + oX, y + oY, t + oT)
      buffer.append((superID(pseudoCellID), new SuperCell(cell, size, base(pseudoCellID))))
    }
    buffer.toArray
  }
}
