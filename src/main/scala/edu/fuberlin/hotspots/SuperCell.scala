package edu.fuberlin.hotspots

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCell(val cells:Map[Cellid, Int], val size:Int, val base:Cellid) extends Serializable{
  def this(singleCell:(Cellid, Int), size: Int, base:Cellid) = {
    this(immutable.HashMap[Cellid, Int](singleCell), size, base)
  }

  def ++(other:SuperCell): SuperCell ={
    assert(this.base == other.base)
    new SuperCell(this.cells ++ other.cells, size,base)
  }

  def neighbours(cellid:Cellid): Seq[Int] = {
    val (x, y, t) = cellid
    val buffer = ListBuffer[Int]()
    for(xO <- -1 to 1; yO <- -1 to 1; tO <- -1 to 1){
      (xO, yO, tO) match {
        case (0, 0, 0) => ;
        case _ => cells.get((x + xO, y + yO, t + tO)) match{
          case Some(value) => buffer.append(value)
          case _ => ;
        }
      }
    }
    buffer.toSeq
  }

  def coreCells:Seq[(Cellid, Int)] = {
    val buffer = ListBuffer[(Cellid, Int)]()
    for(x <- base._1 until base._1 + size; y <- base._2 until base._2 + size; t <- base._3 until base._3 + size){
      cells.get(x, y, t) match{ case Some(value) => buffer.append(((x, y, t), value)) case None => }
    }
    buffer.toSeq
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
