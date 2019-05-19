
import org.joda.time.format.DateTimeFormat

import org.joda.time.LocalTime

import org.joda.time.LocalDate

import org.apache.spark._

import scala.util.matching.Regex

case class flight(date: String, airline: String, flightNumber: String, origin: String,dest: String, dep: String, dep_delay: Double, arv: String,arv_delay: Double, airtime: Double, distance: Double) extends Serializable {}

object Main extends App{

  override def main(arg:Array[String]) : Unit = {
    
 var sparkConf = new SparkConf().setMaster("local").setAppName("flight")
 var sc = new SparkContext(sparkConf)

  var flightrdd = sc.textFile("file:///home/roshni/Downloads/flights.csv")

  var filterrdd = flightrdd.filter(x => !x.contains("YEAR"))

  var newrdd=filterrdd.filter{ x=>

        val removespace:Regex=("[A-Za-z0-9],{2,}[A-Za-z0-9]").r

        removespace.findFirstMatchIn(x)== None

        }

   var data1 = sc.parallelize (newrdd.take(400000))     

  var data = data1.map(parseFlight)

   //var data=newrdd.map(parseFlight)

  var total_Flights=data.count()

  var totalFightDistance=data.map(_.distance).reduce((x,y)=> x+y)

  var total_Dep_Del_Flights=data.filter(_.dep_delay>0).count

  var total_Arv_Del_Flights=data.filter(_.arv_delay>0).count

 var percent_Dely_Flights=(total_Dep_Del_Flights *100)/total_Flights

 var percent_Arv_Flights=(total_Arv_Del_Flights *100)/total_Flights

 var total_Arv_Dely_Distance=data.filter(_.arv_delay>0).map(_.arv_delay).reduce(_+_)

 var average_Arv_Dely=total_Arv_Dely_Distance / total_Arv_Del_Flights

 var total_Dep_Dely_Distance=data.filter(_.dep_delay>0).map(_.dep_delay).reduce(_+_)

 var average_Dep_Dely=total_Dep_Dely_Distance / total_Dep_Del_Flights


var group_Rdd = data.map(w =>(w.airline,w)).groupByKey()

 var noofdelayedflightofairlines = group_Rdd.map{ case (key,value) => (key,value.filter(_.dep_delay>0))}.collect

 var countofdelayedflightofairlines = noofdelayedflightofairlines.map{ case (key,value) => (key,value.size)} toList

 var noofflightofairlines = group_Rdd.map{ case (key,value) => (
if (key.contains("B6")){key.replace("B6","JetBlue Airways")} 
else if (key.contains("UA")){key.replace("UA","United airlines_Inc")} 
else if (key.contains("AA")){key.replace("AA","American airlines_Inc")} 
else if (key.contains("US")){key.replace("US","US Airways_Inc")} 
else if (key.contains("F9")){key.replace("F9","Frontier Airlines_Inc")} 
else if (key.contains("OO")){key.replace("OO","Skywest Airlines_Inc")} 
else if (key.contains("AS")){key.replace("AS","Alaska Airlines_Inc")} 
else if (key.contains("NK")){key.replace("NK","Spirit Airlines_Inc")} 
else if (key.contains("WN")){key.replace("WN","Southwest AirlinesCo.")} 
else if (key.contains("DL")){key.replace("DL","Delta Air Lines_Inc")} 
else if (key.contains("EV")){key.replace("EV","Atlantic SoutheastAirlines")} 
else if (key.contains("HA")){key.replace("HA","Hawaiian Airlines_Inc")} 
else if (key.contains("MQ")){key.replace("MQ","American Eagle Airlines_Inc")} 
else if (key.contains("")) {key.replace("VX","virgin America")}
,value.size)}.collect.toList


 var AirlineDatawithDelays =countofdelayedflightofairlines.zip(noofflightofairlines).map {case ((a,b),(e,d)) => (a,b,d)} toList

 val res = AirlineDatawithDelays.map {case ((a,b,c)) => (a,b,c,(b*100.0)/c)}

var delay_dep_flights=group_Rdd.map{ case (key,value) => (key,value.filter(_.dep_delay>0))}.collect

var sum_delay_dep_flights=delay_dep_flights.map{ case (key,value) => (key,value.map(_.dep_delay).sum)}.toList




val airline_avg_flights=countofdelayedflightofairlines.zip(sum_delay_dep_flights).map{case((a,b),(c,d))=>(a,b,d)}.toList

val avg_delay_dep=airline_avg_flights.map{case((a,b,c))=>(a,c/b)}.toList


val finaldata=noofflightofairlines.zip(AirlineDatawithDelays).zip(res).zip(avg_delay_dep).toList

val output_Data=finaldata.map{case ((((a,b),(c,d,e)),(f,g,h,i)),(j,k))=> (f"Airline_Company-$a%25s",f"  Total_Flights-$b%5s",f" Delayed_Flights-$d%5s",f" Percentage_Delayed--$i%.2f"+"%",f" Average_Delayed--$k%.1f")}

//printing statements 
println("flight_total_Distance  =" +totalFightDistance)
println("number of dep_Del_Flights="+total_Dep_Del_Flights)
println("number of arv_Del_Flights   ="+total_Arv_Del_Flights)
println(f"percent_Dep_Delayed        =$percent_Dely_Flights%.1f"+"%")
println(f"percent_Arv_Delayed        =$percent_Arv_Flights%.1f"+"%")
println(f"average_Dep_Delayed        =$average_Dep_Dely%.2f")
println(f"average_Arv_delayed        =$average_Arv_Dely%.2f")
println()
output_Data.foreach(println)

sc.stop()

}

  def parseFlight(row: String): flight = {

  val fields = row.split(",")

  val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")

  val timepattern = DateTimeFormat.forPattern("HHmm")

  val date: String = fields(0)+"-"+fields(1)+"-"+fields(2)

  val airline: String = fields(4)

  val flightNumber: String = fields(5)

  val origin: String = fields(7)

  val dest: String = fields(8)

  if(fields(10)=="2400") fields(10) ="0000"

  val dep: String = fields(10)

  val dep_delay: Double = fields(11).toDouble

  if(fields(21)=="2400") fields(21) ="0000"

  val arv: String = fields(21)

  val arv_delay: Double = fields(22).toDouble

  val airtime: Double = fields(16).toDouble

  val distance: Double = fields(17).toDouble

  flight(date, airline, flightNumber, origin, dest, dep, dep_delay, arv, arv_delay, airtime, distance)

}
}

