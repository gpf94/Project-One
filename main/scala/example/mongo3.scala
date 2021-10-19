package example

import scala.io.Source;
import java.io.File;
import java.io._;
import scala.io.StdIn.readLine;


import org.mongodb.scala._;
import org.mongodb.scala.model.Filters._;
import example.Helpers._;
import java.io._;
import scala.io.StdIn.readLine;

object mongo3 extends App {
    val client: MongoClient = MongoClient();
    val database: MongoDatabase = client.getDatabase("testDB");
    val collection: MongoCollection[Document] = database.getCollection("testCollection");

import scala.io.Source
val StringDoc = Source.fromFile("src/updatedwinedatabase.json").getLines.toList;
val bsonDoc = StringDoc.map(doc => Document(doc))
collection.insertMany(bsonDoc).printResults();
}