package example

import example.Helpers._;

import scala.io.Source;
import java.io.File;
import java.io._;
import scala.io.StdIn.readLine;
import org.mongodb.scala._;
import org.mongodb.scala.model.Filters._

import java.io._;
import scala.io.StdIn.readLine;
object zip extends App {
    val client: MongoClient = MongoClient();
    val database: MongoDatabase = client.getDatabase("geography2");
        // Get a Collection.
    val collection: MongoCollection[Document] = database.getCollection("zipcodes");
    // How to read from json file to mongodb
    val stringDoc = Source.fromFile("src/zipcodes.json").getLines.toList;
    val bsonDoc = stringDoc.map(doc => Document(doc));
    collection.insertMany(bsonDoc).printResults();
}