import org.apache.spark._
import scala.util.matching.Regex

object Main extends App{
override def main(arg:Array[String]) : Unit = {
var sparkConf = new SparkConf().setMaster("local").setAppName("crime")
var sc = new SparkContext(sparkConf)
sc.setLogLevel("ERROR")
  
//access location of file
var raw_Rdd=sc.textFile("file:///home/roshni/Downloads/project/CrimeData/Crime_dataset")
var data = raw_Rdd.map(parse)

//no of crimes under each fbi code
var pair_Fbicode=data.map(x=> ((x.fbi_Code),x.case_No)).groupByKey().collect
var no_Of_Cases=pair_Fbicode.map{ case (key,value)=> (f"FbiCode=$key%5s"," crime_No= "+value.size)}.foreach(println)

//no of theft arrests happened in district
var Filter1=data.filter(x=> (x.primary_Type==("THEFT")&& x.arrest==("true")))
var req_pair=Filter1.map(x=> ((x.district),x.primary_Type)).groupByKey()
var output_size=req_pair.map{case(key,value)=>(f"district=$key%3s"," no_Of_Arrest= "+value.size)}.foreach(println)

//no of cases under nacrotics 
var narcos_cases=data.filter(x=> (x.primary_Type==("NARCOTICS"))).count()

//location crime rate is higher
var crime_pair=data.map(x=> ((x.ll_Location,x.primary_Type))).groupByKey()

//location under fbi code have max no of crimes
var fbi_pair=data.map(x=> ((x.district),x.fbi_Code)).groupByKey()
var result=fbi_pair.map{case (key,value)=> (f"district _no=$key%2s"," crime_No="+value.size)}

//printing statements
println("no_Of_Crimes_underFbiCode")
println(no_Of_Cases)
println("no_Of_Arrest_theft")
println(output_size)
println("no_Of_Cases_under_narcotics= "+narcos_cases)
println("location crime rate is higher"+result.max)
sc.stop()

}

  case class crime(case_Id:Int,case_No:String,crime_Date:String,crime_Block:String,id_Iucr:String,primary_Type:String,descriptio:String,loc_Description:String,arrest:String,domestic:String,beat:Int,district:Int,ward:Int,community:Int,fbi_Code:String,x_Ordinates:Int,y_Ordinates:Int,year:Int,updated_On:String,lattitude:String,longitutude:String,ll_Location:String) extends Serializable{}

 def parse(row:String):crime={
 val field= row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1)
 val case_Id:Int=field(0).toInt
 val case_No =field(1)
 val crime_Date:String=field(2)
 val crime_Block:String=field(3)
 val id_Iucr=field(4)
 val primary_Type:String=field(5)
 val descriptio:String=field(6)
 val loc_Description:String=field(7)
 val arrest=field(8)
 val domestic:String=field(9)
 val beat:Int=field(10).toInt
 val district:Int=field(11).toInt
 val ward:Int=field(12).toInt
 val community:Int=field(13).toInt
 val fbi_Code:String=field(14)
 if (field(15)=="") field(15)="0000000"
 val x_Ordinates:Int=field(15).toInt
 if (field(16)=="") field(16)="0000000"
 val y_Ordinates:Int=field(16).toInt
 val year:Int=field(17).toInt
 val updated_On:String=field(18)
 if (field(19)=="") field(19)="00.000000000"
 val lattitude:String=field(19)
 if (field(20)=="") field(20)="00.000000000"
 val longitutude:String=field(20)
 val ll_Location:String=field(21)

crime(case_Id,case_No,crime_Date,crime_Block,id_Iucr,primary_Type,descriptio,loc_Description,arrest,domestic,beat,district,ward,community,fbi_Code,x_Ordinates,y_Ordinates,year,updated_On,lattitude,longitutude,ll_Location)

}
}
