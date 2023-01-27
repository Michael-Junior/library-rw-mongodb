import org.mongodb.scala.bson.{BsonValue, ObjectId}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.model.{Aggregates, Filters}
import org.mongodb.scala.result.{InsertManyResult, InsertOneResult}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, Observable, ObservableFuture}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.{Failure, Success, Try}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

class MongodbRW(dataBase: String,
                collection: String,
                host: Option[String] = None,
                port: Option[Int] = None,
                user: Option[String] = None,
                password: Option[String] = None,
                idField: Option[String] = None,
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

  private val coll: MongoCollection[Document] = if (existsCollection(dbase)) {
    if append then dbase.getCollection(collection)
    else throw Exception("ERROR: Collection already exists, change parameter 'append'")
  } else {
    dbase.createCollection(collection).results()
    dbase.getCollection(collection)
  }

  def findAll(quatity: Int = 0, outputFields: String = ""): Try[Seq[Document]] = {
    Try {
      val docsResults: Seq[Document] = new DocumentObservable(coll.find().limit(quatity)).observable.results()
      if outputFields.isEmpty then docsResults else fieldsFilter(docsResults, outputFields)
    }
  }

  def findQuery(query: String, quatity: Int = 0, outputFields: String = ""): Try[Seq[Document]] = {
    Try {
      val docsResults: Seq[Document] = new DocumentObservable(coll.find(Document(query)).limit(quatity)).observable.results()
      if outputFields.isEmpty then docsResults else fieldsFilter(docsResults, outputFields)
    }
  }

  def insertDocument(doc: String): Try[Seq[InsertOneResult]] = {
    Try {
      val objectIdDocumentsInserted = coll.insertOne(Document(doc)).results()

      if idField.getOrElse("").nonEmpty then objectIdDocumentsInserted.foreach(f => checkDuplicateIdField(f.getInsertedId.asObjectId().getValue.toString, idField.getOrElse("")))
      objectIdDocumentsInserted
    }
  }

  def insertDocuments(docs: Seq[String]): Try[Seq[InsertManyResult]] = Try {
    val objectIdDocumentsInserted: Seq[InsertManyResult] = coll.insertMany(docs.map(f => Document(f))).results()

    if idField.getOrElse("").nonEmpty then {
      objectIdDocumentsInserted.foreach(f => f.getInsertedIds.values().forEach(f => checkDuplicateIdField(f.asObjectId().getValue.toString, idField.getOrElse(""))))
    }
    objectIdDocumentsInserted
  }

  private def checkDuplicateIdField(idDocumentsInserted: String, idField: String): Unit = {

    val comandMongoOid: String = "$oid"
    val query: String = s"{_id:{$comandMongoOid:'$idDocumentsInserted'}}"
    val documentsInserted: Seq[Document] = findQuery(query, 1).get
    val idFieldValue = documentsInserted.map(f => f.filterKeys(x => x.contains(idField))).head.map(f => f._2.asString().getValue).head
    val isDuplicate: Boolean = isDuplicateId(idField, idFieldValue)

    if (isDuplicate) {
      val generatedNewId = new ObjectId().toString
      val isDuplicateNewId = isDuplicateId(idField, generatedNewId)

      if (!isDuplicateNewId) {
        val documentsDuplicate: Seq[Document] = getDuplicateDocuments(idField, idFieldValue)
        val documentsDuplicateFilter = documentsDuplicate.map(f => f.filterKeys(x => x.contains(idField)))
        documentsDuplicateFilter.slice(1, documentsDuplicateFilter.length).map(f => f.map(f => updateOne(idField, f._2.asString().getValue, generatedNewId)))
      }
    }
  }

  private def updateOne(nameField: String, idDuplicate: String, idNew: String): Unit = {
    coll.updateOne(equal(nameField, idDuplicate), combine(set(nameField, idNew))).results()
    println(idNew)
  }

  private def isDuplicateId(nameField: String, id: String): Boolean = {
    coll.aggregate(Seq(Aggregates.filter(Filters.equal(nameField, id)))).results().length >= 2
  }

  private def getDuplicateDocuments(nameField: String, id: String): Seq[Document] = {
    coll.aggregate(Seq(Aggregates.filter(Filters.equal(nameField, id)))).results()
  }

  private def fieldsFilter(docsResults: Seq[Document], outputFields: String): Seq[Document] = {

    val fields: Array[String] = outputFields.split(",")
    docsResults.map(f => f.filterKeys(x => fields.contains(x))) //Ler campo a campo e gerar um log caso a chave não exista
  }

  private def existsCollection(dbase: MongoDatabase): Boolean = {
    val listCollection = dbase.listCollectionNames().results()
    listCollection.contains(collection)
  }

  private implicit class DocumentObservable(val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: Document => String = doc => doc.toJson()
  }

  private implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: C => String = doc => Option(doc).map(_.toString).getOrElse("")
  }

  private trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: C => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(120, TimeUnit.SECONDS))

    private def headResult(): C = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))

    def printResults(initial: String = ""): Unit = {
      if (initial.nonEmpty) print(initial)
      results().foreach(res => println(converter(res)))
    }

    def printHeadResult(initial: String = ""): Unit = println(s"$initial${converter(headResult())}")
  }
}