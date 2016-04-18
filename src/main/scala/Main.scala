/**
  * Created by nabegh.althalji on 15/04/2016.
  */
import akka.actor.{Actor, ActorSystem, Props}

import com.typesafe.config._

import java.sql.DriverManager
import java.util.Calendar

import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try



class ScheduleActor extends Actor {
  import context.dispatcher

  val tick = context.system.scheduler.schedule(1 second, 3 minutes, self, "tick")

  var lastMV = "2016-01-01 00:00:00"
  var lastOrder = "2016-01-01 00:00:00"

  override def postStop() = tick.cancel()

  def receive = {
    case "tick" =>
      // do something useful here
      val updateValues = DBObject.getLastValue
      val updateMV = updateValues(0).getOrElse("2016-01-01 00:00:00")
      val updateOrder = updateValues(1).getOrElse("2016-01-01 00:00:00")

      if(lastMV != updateMV) {
        println(s"[${Calendar.getInstance().getTime()}]: Last updated MV at $updateMV")
        lastMV = updateMV
      } else {
        println(s"[${Calendar.getInstance().getTime()}]: No MV change")
      }

      if(lastOrder != updateOrder) {
        println(s"[${Calendar.getInstance().getTime()}]: Last updated order at $updateOrder")
        lastOrder = updateOrder
      } else {
        println(s"[${Calendar.getInstance().getTime()}]: No order change")
      }
  }
}

object DBObject {
  def getLastValue(): List[Option[String]] = {

    val conf = ConfigFactory.load()
    val DBPath = conf.getString("vertica.path")
    val DBUser = conf.getString("vertica.user")
    val DBPass = conf.getString("vertica.pass")


    val conn = Try(DriverManager.getConnection(DBPath, DBUser, DBPass))
    var results: List[Option[String]] = List()
    if(conn.isSuccess) {
      //execute statement
      val source = Source.fromFile("SQL/LastUpdate.sql")
      val statementString = source.mkString
      val rs = conn.get.prepareStatement(statementString).executeQuery()
      while (rs.next) {
        results = List(Some(rs.getString("LAST_MV")), Some(rs.getString("LAST_ORDER")))
      }
      source.close
      results
    } else {
      println(s"Problem connecting: ${conn.failed.get.getMessage}")
      List()
    }
  }
}

object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("BGT3-Scheduler")
    val scheduled = system.actorOf(Props[ScheduleActor], "scheduledActor")
  }
}
