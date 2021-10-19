package example

import org.mongodb.scala._

import example.Helpers._

object MongoDemo {
  def main(args: Array[String]) {
    println("Starting MongoDB - Scala Demo...")
  
    val client: MongoClient = MongoClient()
    val database: MongoDatabase = client.getDatabase("ptesting")
    // Get a Collection.
    val collection: MongoCollection[Document] = database.getCollection("pcollection")


    val doc: Document = Document(
      "_id" -> 0,
      "name" -> "MongoDB",
      "type" -> "database",
      "count" -> 1,
      "info" -> Document("x" -> 203, "y" -> 102)
    )
    //println(doc)
    println("Printing results...")
    collection.insertOne(doc).results()
  }
}