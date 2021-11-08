package example


import java.io.IOException

import scala.util.Try

import scala.io.StdIn.{readLine,readInt}

// Demo1.
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

// // Demo2.
// import org.apache.hadoop.hive.cli.CliSessionState
// import org.apache.hadoop.hive.conf.HiveConf
// import org.apache.hadoop.hive.ql.Driver
// import org.apache.hadoop.hive.ql.session.SessionState
// import java.sql.Driver

object HiveDemo {
  def main(args: Array[String]) {
    println("Starting Hive Demo...")

    //demo1()
    demo2()

  }

  def demo1(): Unit = {

    //val hiveClient = new HiveClient
    //hiveClient.executeHQL("SELECT * FROM trucks")
    // hiveClient.executeHQL("DROP TABLE IF EXISTS DEMO")
    // hiveClient.executeHQL("CREATE TABLE IF NOT EXISTS DEMO(id int)")
    // hiveClient.executeHQL("INSERT INTO DEMO VALUES(1)")
    // hiveClient.executeHQL("SELECT * FROM DEMO")
    //hiveClient.executeHQL("SELECT COUNT(*) FROM DEMO")
  }

  def demo2(): Unit = {

    var con: java.sql.Connection = null;
    try {
      // For Hive2:
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

      // For Hive1:
      //var driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"
      //val conStr = "jdbc:hive://sandbox-hdp.hortonworks.com:10000/default";

      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val stmt = con.createStatement();

      println("Welcome to the NBA app!")
      var username = readLine("Enter your username: ")
      var password = readLine("Enter your password: ")
      println(s"Welcome back, $username!")

      println("Showing live games scores...")
      var res = stmt.executeQuery("SELECT * FROM nba1")
      while (res.next()) {
        println(s"${res.getString(1)}, ${res.getString(2)}, ${res.getString(3)}, ${res.getString(4)}, ${res.getString(5)}, ${res.getString(6)}")
      }

      var gameNumber = readLine("Enter the game number for the game whose box score you would like to inspect: ")
      var res2 = stmt.executeQuery(s"Select player_id, player_name, team_name, pts from game$gameNumber order by team_name, pts desc")
      while (res2.next()) {
        println(s"${res2.getString(1)}, ${res2.getString(2)}, ${res2.getString(3)}, ${res2.getString(4)}")
      }

      var input = readLine("Would like to compare top scorers for all games tonight (yes/no)?: ")
      if (input == "yes") {
        val res6 = stmt.executeQuery("select a.player_name, a.team_name, a.pts from game473547 a where pts > 19 union all select b.player_name, b.team_name, b.pts from game473548 b where pts > 19 union all select c.player_name, c.team_name, c.pts from game473549 c where pts > 19 union all select e.player_name, e.team_name, e.pts from game473550 e where pts > 19 union all select f.player_name, f.team_name, f.pts from game473551 f where pts > 19 union all select g.player_name, g.team_name, g.pts from game473552 g where pts > 19 union all select h.player_name, h.team_name, h.pts from game473553 h where pts > 19 union all select i.player_name, i.team_name, i.pts from game473554 i where pts > 19 order by pts desc")
        while (res6.next()) {
        println(s"${res6.getString(1)}, ${res6.getString(2)}, ${res6.getString(3)}")
      }
      } else println("Moving onto the next query...")

      var mainMenu = readLine("Return to main menu (yes/no)?: ")
      var res11 = stmt.executeQuery("SELECT * FROM nba1")
      while (res11.next()) {
        println(s"${res11.getString(1)}, ${res11.getString(2)}, ${res11.getString(3)}, ${res11.getString(4)}, ${res11.getString(5)}, ${res11.getString(6)}")
      }
      var gameNumber4 = readLine("Enter the game number for the game you would like to check for hothand (pts/min): ")
      var res5 = stmt.executeQuery(s"select player_id, player_name, team_name, (pts / min) as hothand from game$gameNumber4 order by team_name, hothand desc")
      while (res5.next()) {
        println(s"${res5.getString(1)}, ${res5.getString(2)}, ${res5.getString(3)}, ${res5.getString(4)}")
      }

      var mainMenu2 = readLine("Return to main menu (yes/no)?: ")
      var res12 = stmt.executeQuery("SELECT * FROM nba1")
      while (res12.next()) {
        println(s"${res12.getString(1)}, ${res12.getString(2)}, ${res12.getString(3)}, ${res12.getString(4)}, ${res12.getString(5)}, ${res12.getString(6)}")
      }
      var gameNumber2 = readLine("Enter the game number for the game whose 3-point shooting data(makes, attempts, and percentage) you would like to inspect: ")
      var res3 = stmt.executeQuery(s"Select player_id, player_name, team_name, fg3m, fg3a, fg3_pct from game$gameNumber2 order by team_name, fg3m desc")
      while (res3.next()) {
        println(s"${res3.getString(1)}, ${res3.getString(2)}, ${res3.getString(3)}, ${res3.getString(4)}, ${res3.getString(5)}, ${res3.getString(6)}")
      }

      var mainMenu3 = readLine("Return to main menu (yes/no)?: ")
      var res13 = stmt.executeQuery("SELECT * FROM nba1")
      while (res13.next()) {
        println(s"${res13.getString(1)}, ${res13.getString(2)}, ${res13.getString(3)}, ${res13.getString(4)}, ${res13.getString(5)}, ${res13.getString(6)}")
      }
      var gameNumber3 = readLine("Enter the game number for which you would like to inspect player efficiencies: ")
      var res4 = stmt.executeQuery(s"select player_name, team_name, (pts + reb + stl + blk - (fga - fgm) - (fta - ftm) - turnover) as efficiency from game$gameNumber3 order by team_name, efficiency desc, player_name")
      while (res4.next()) {
        println(s"${res4.getString(1)}, ${res4.getString(2)}, ${res4.getString(3)}")
      }



      println("Hope you enjoyed the app...")



    } catch {
      case ex => {
        ex.printStackTrace();
        throw new Exception(s"${ex.getMessage}")
      }
    } finally {
      try {
        if (con != null)
          con.close();
      } catch {
        case ex => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }
  }
}

// // Hive meta API client.
// class HiveClient {

//   val hiveConf = new HiveConf(classOf[HiveClient])

//   // Get the hive ql driver to execute ddl or dml
//   private def getDriver: Driver = {
//     println(s"Getting Driver...")
//     val driver = new Driver(hiveConf)
//     SessionState.start(new CliSessionState(hiveConf))
//     driver
//   }

//   def executeHQL(hql: String): Int = {
//     println(s"Executing query $hql ...")
//     //val responseOpt = Try(getDriver.run(hql)).toEither
//     // val response = responseOpt match {
//     //   case Right(response) => response
//     //   case Left(exception) => throw new Exception(s"${ exception.getMessage }")
//     // }
//     try {
//       val responseOpt = getDriver.run(hql)
//       val response = responseOpt

//       val responseCode = response.getResponseCode
//       if (responseCode != 0) {
//         val err: String = response.getErrorMessage
//         throw new IOException(
//           "Failed to execute hql [" + hql + "], error message is: " + err
//         )
//       }
//       responseCode

//     } catch {
//       case ex => {
//         throw new Exception(s"${ex.getMessage}")
//       }
//     }
//   }

// }