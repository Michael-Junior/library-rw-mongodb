import org.mongodb.scala.result.{InsertManyResult, InsertOneResult}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, Observable, ObservableFuture}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import java.util.concurrent.TimeUnit

class mongodbRW(dataBase: String,
                collection: String,
                host: Option[String] = None,
                port: Option[Int] = None,
                user: Option[String] = None,
                password: Option[String] = None,
                idField: Option[String],
                append: Boolean = false) {
  require((user.isEmpty && password.isEmpty) || (user.nonEmpty && password.nonEmpty))

  private val hostStr: String = host.getOrElse("localhost")
  private val portStr: String = port.getOrElse(27017).toString
  private val usrPswStr: String = user match {
    case Some(usr) => s"$usr:${password.get}@"
    case None => ""
  }

  private val mongoUri: String = s"mongodb://$usrPswStr$hostStr:$portStr"
  private val mongoClient: MongoClient = MongoClient(mongoUri)
  private val dbase: MongoDatabase = mongoClient.getDatabase(dataBase)
  private val coll: MongoCollection[Document] = dbase.getCollection(collection)


  def findAll(quatity: Int = 0, outputFields: String = ""): Try[Seq[Document]] = {
    Try {
      val docsResults: Seq[Document] = new DocumentObservable(coll.find().limit(quatity)).observable.results()

      outputFields.isEmpty match
        case true => docsResults
        case false => Fieldsfilter(docsResults, outputFields)
    }
  }

  def findQuery(query: String, quatity: Int = 0, outputFields: String = ""): Try[Seq[Document]] = {
    Try {
      val docsResults: Seq[Document] = new DocumentObservable(coll.find(Document(query)).limit(quatity)).observable.results()

      outputFields.isEmpty match
        case true => docsResults
        case false => Fieldsfilter(docsResults, outputFields)
    }
  }

  def insertDocument(doc: String): Try[Seq[InsertOneResult]] = Try { coll.insertOne(Document(doc)).results() }

  def insertDocuments(docs: Seq[String]): Try[Seq[InsertManyResult]] = Try { coll.insertMany(docs.map(f => Document(f))).results() }

  def Fieldsfilter(docsResults: Seq[Document], outputFields: String): Seq[Document] = {

    val fields: Array[String] = outputFields.split(",")
    docsResults.map(f => f.filterKeys(x => fields.contains(x)))
  }

  implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: Document => String = doc => doc.toJson()
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: C => String = doc => Option(doc).map(_.toString).getOrElse("")
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: C => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(120, TimeUnit.SECONDS))

    def headResult(): C = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))

    def printResults(initial: String = ""): Unit = {
      if (initial.nonEmpty) print(initial)
      results().foreach(res => println(converter(res)))
    }

    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }
}