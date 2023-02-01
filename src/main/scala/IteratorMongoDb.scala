import org.mongodb.scala.Document
import org.mongodb.scala.bson.Document

import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer

class IteratorMongoDb[T](docs: Seq[T]) extends Iterator[T] {


  private val batchDocuments: ListBuffer[T] = ListBuffer.empty ++= docs
  private val currentSize: Int = batchDocuments.length
  private var currentIndex : Int = 0

  override def hasNext: Boolean = currentIndex < currentSize && batchDocuments(currentIndex) != 0

  override def next() = {
    val t = batchDocuments(currentIndex)
    currentIndex = currentIndex + 1
    t
  }
}

object TestIteratorMongoDb extends App{

  private val cursor = new IteratorMongoDb[String](Seq("A", "B", "C", "D", "E"))
  private val batchDocuments: ListBuffer[String] = new ListBuffer[String]()
  private val bufferSize: Int = 3

  while (cursor.hasNext) {
    batchDocuments += cursor.next()
    if (batchDocuments.length % bufferSize == 0 || (!cursor.hasNext && !(batchDocuments.length % bufferSize == 0))) { //processes bufSize or the rest
      batchDocuments.foreach(f => println(f))
      batchDocuments.clear()
    }
  }
}