
import org.apache.spark._

import scala.util.matching.Regex

object Main extends App{

  override def main(arg:Array[String]) : Unit = {
    
 var sparkConf = new SparkConf().setMaster("local").setAppName("emergency")
 var sc = new SparkContext(sparkConf)

        var sparkConf = new SparkConf().setMaster("local").setAppName("EmergencyHelpline")
        var sc = new SparkContext(sparkConf)
    //access location of file//
        var raw_Rdd=sc.textFile("file:///home/roshni/Downloads/project/EmergencyHelpline/911.csv").filter(x=> (!x.contains("lat")))
        var raw=raw_Rdd.filter{ x=>
        val removespace:Regex=(",,").r
           
        removespace.findFirstMatchIn(x)== None
        }
       var data=raw.map(parse1)
       var pair_911=data.map(x=>(x.zip,x.title))

 var rdd1=sc.textFile("file:///home/roshni/Downloads/project/EmergencyHelpline/zipcode.csv").filter(x=> (!x.contains("zip")))
 var removequotesrdd = rdd1.map(x => x.replace('"', ' ').trim())
 var Data2 = removequotesrdd.map(x => x.replace(" ","").trim())
 var data2=Data2.map(parse2)
 var pair1=data2.map(x=>(x.zip,x.state))
 var pair2=data2.map(x=>(x.zip,x.city))

var finalstate_rdd=pair_911.join(pair1)
var finalcity_rdd=pair_911.join(pair2)

var final_state_pair1=finalstate_rdd.map{case (a,(b,c))=> (f"state=$c%4s",f"cases_prevelent$b%7s")}.countByValue().foreach(println)
var final_state_pair2=finalcity_rdd.map{case (a,(b,c))=> ("city= "+c,b)}.countByValue().foreach(println)

//printing statements
println(final_state_pair1)
println(final_state_pair2)
sc.stop()


}

 case class Emergency1(latitude: Double, longitude: Double, description: String, zip: Int, title: String, timestamp: String, twp: String, address: String, e:Int)
   def parse1(row: String): Emergency1 = {
   val fields = row.split(",")
   val latitude: Double = fields(0).toDouble
   val longitude: Double = fields(1).toDouble
   val description: String = fields(2)
   val zip: Int = fields(3).toInt
   val title: String = fields(4).substring(0,fields(4).indexOf(":")).toString()
   val timestamp: String = fields(5)
   val twp: String = fields(6)
   val address: String = fields(7)
   val e :Int = fields(8).toInt   
    Emergency1(latitude,longitude,description,zip,title,timestamp,twp,address,e)
   }  

 case class Emergency2(zip: Int, city: String, state: String,latitude: Double, longitude: Double, timezone: Int, dst: Int)
 def parse2(row: String): Emergency2 = {
   val fields = row.split(",")
   val zip: Int = fields(0).toInt
   val city: String = fields(1)
   val state: String = fields(2)
   val latitude: Double = fields(3).toDouble
   val longitude: Double = fields(4).toDouble
   val timezone: Int = fields(5).toInt
   val dst: Int= fields(6).toInt 
    Emergency2(zip,city,state,latitude,longitude,timezone,dst)  }

}

