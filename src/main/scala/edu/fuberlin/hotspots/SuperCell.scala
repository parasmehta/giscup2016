package edu.fuberlin.hotspots

import it.unimi.dsi.fastutil.ints.{Int2IntAVLTreeMap, Int2IntMap}

import scala.collection.mutable.ListBuffer

/**
  * A supercell is a cube containing cells (A cell consists of its ID and the number of passenger dropped off there).
  * The design goal of this class is to allow every cell inside the super cell to access all of its neighbours.
  * Therefore, a supercell also contains buffer cells. These do not actually belong to the supercell and just serve
  * as neighbours to the real core cells
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCell(val cells:Int2IntMap, val size:Int, val base:Cellid) extends Serializable{
  def this(cellSeq:Seq[(Int, Int)], size: Int, base:Int){
    this(new Int2IntAVLTreeMap(cellSeq.map(_._1).toArray, cellSeq.map(_._2).toArray), size, decompose(base))
  }

  /**
    * get the a list of the number of passengers dropped off in the neighbours.
    * It does not return the cellids of the neighbours
    * @param cellid
    * @return
    */
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

  /**
    * List of cells without the buffer cells
    * @return
    */
  def coreCells:Seq[(Cellid, Int)] = {
    val buffer = ListBuffer[(Cellid, Int)]()
    for(x <- base._1 until base._1 + size; y <- base._2 until base._2 + size; t <- base._3 until base._3 + size){
      get(x, y, t) match{ case -1 => ; case passengerCount => buffer.append(((x, y, t), passengerCount))}
    }
    buffer
  }

  /**
    * get a cell for the given cellid. Composed cellids are used in the internal.
    * This reduces the memory by havin smaller keys and allows us to take advantage of the
    * fastutils library.
    * @param x
    * @param y
    * @param t
    * @return
    */
  def get(x:Int, y:Int, t:Int):Int = {
    val key = compose(x, y, t)
    cells.containsKey(key) match { case true => cells.get(key) case false => -1 }
  }
}

/**
  * The task of this class is to determine to which supercell a cell belongs.
  * A cell can be at the edge or even at the corner of a supercell.
  * If that turns out to be true, the this cell belongs to multiple supercells. As a core cell to only one supercell
  * and a buffer cell in 1 up to 8 different supercells.
  * @param size
  */
class SuperCellFactory(val size:Int) extends Serializable {
  private def base(cellID:Cellid):Cellid = {
    val (x, y, t) = cellID
    ((x / size) * size, (y / size) * size, (t / size) * size)
  }

  private def superID(cellID:Cellid):Cellid = {
    (cellID._1 - (cellID._1 % size), cellID._2 - (cellID._2 % size), cellID._3 - (cellID._3 % size))
  }

  private def offsets(coordinate:Long):Seq[Int] = {
    coordinate match{
      case 0 => Seq(0)
      case c => c % size match{
        case 0 => Seq(-1, 0)
        case mod if mod == size - 1  => Seq(0, 1)
        case _ => Seq(0)
      }
    }
  }

  def create(cell:(Int, Int)): Seq[(Int, (Int, Int))] = {
    val (x, y, t) = decompose(cell._1)
    val (offX, offY, offT) = (offsets(x), offsets(y), offsets(t))
    val buffer = new ListBuffer[(Int, (Int, Int))]()
    for(oX <- offX; oY <- offY; oT <- offT){
      val pseudoCellID = (x + (oX * size), y + (oY * size), t + (oT * size))
      buffer.append((compose(superID(pseudoCellID)), cell))
    }
    buffer.toSeq
  }
}
