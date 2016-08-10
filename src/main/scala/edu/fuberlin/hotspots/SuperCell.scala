package edu.fuberlin.hotspots

import it.unimi.dsi.fastutil.ints.{Int2IntAVLTreeMap, Int2IntMap}

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCell(val cells:Int2IntMap, val size:Int, val base:Cellid) extends Serializable{
  def this(cellSeq:Seq[(Int, Int)], size: Int, base:Int){
    this(new Int2IntAVLTreeMap(cellSeq.map(_._1).toArray, cellSeq.map(_._2).toArray), size, decompose(base))
  }
  def neighbours(cellid:Cellid): Seq[Int] = {
    val (x, y, t) = cellid
    val buffer = ListBuffer[Int]()
    for(xO <- -1 to 1; yO <- -1 to 1; tO <- -1 to 1){
      get(x + xO, y + yO, t + tO) match {
        case -1 => ;
        case passengerCount => buffer.append(passengerCount)
      }
    }
    buffer.toSeq
  }

  def coreCells:Seq[(Cellid, Int)] = {
    val buffer = ListBuffer[(Cellid, Int)]()
    for(x <- base._1 until base._1 + size; y <- base._2 until base._2 + size; t <- base._3 until base._3 + size){
      get(x, y, t) match{ case -1 => ; case passengerCount => buffer.append(((x, y, t), passengerCount))}
    }
    buffer
  }

  def get(x:Int, y:Int, t:Int):Int = {
    val key = compose(x, y, t)
    cells.containsKey(key) match { case true => cells.get(key) case false => -1 }
  }
}

class SuperCellFactory(val size:Int) extends Serializable {
  private def base(cellID:Cellid):Cellid = {
    val (x, y, t) = cellID
    val xcor = x match {case x if x % size == 0 => x case x => ((x / size) - 1) * size}
    (xcor, (y / size) * size, (t / size) * size)
  }

  private def superID(cellID:Cellid):Cellid = {
    (cellID._1 - ((size + cellID._1 % size) % size), (cellID._2 - (cellID._2 % size)), cellID._3 - (cellID._3 % size))
  }

  private def offsets(coordinate:Long):Seq[Int] = {
    coordinate % size match {
      case 0 => Seq(-1, 0)
      case mod if mod == size - 1  => Seq(0, 1)
      case _ => Seq(0)
    }
  }

  def create(cell:(Int, Int)): Seq[(Int, (Int, Int))] = {
    val (x, y, t) = decompose(cell._1)
    val (offX, offY, offT) = (offsets((x % size) + size), offsets(y), offsets(t))
    val buffer = new ListBuffer[(Int, (Int, Int))]()
    for(oX <- offX; oY <- offY; oT <- offT){
      val pseudoCellID = (x + (oX * size), y + (oY * size), t + (oT * size))
      buffer.append((compose(superID(pseudoCellID)), cell))
    }
    buffer.toSeq
  }
}
