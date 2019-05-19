
import scala.util.matching.Regex

import org.apache.spark._

object Main extends App{

  override def main(arg:Array[String]) : Unit = {
    
var sparkConf = new SparkConf().setMaster("local").setAppName("bank")
var sc = new SparkContext(sparkConf)
sc.setLogLevel("ERROR")
    //access location of file
var rdd=sc.textFile("file:///home/roshni/Downloads/project/bank/Transaction Sample data-1.csv")
var filter1=rdd.filter(x=> !x.contains("Account_id"))
val data1=filter1.map(parse1)
val total_transactions=data1.count()
val sum_of_transactions=data1.map(_.amount).reduce(_+_)
val average_sys_1=sum_of_transactions/total_transactions
val join1=data1.map(x=> ((x.account_Id),x))

var rdd1=sc.textFile("file:///home/roshni/Downloads/project/bank/Transaction Sample data-2.csv")
val fill1=rdd1.filter(x=> !x.contains("Account_id"))
val data2=fill1.map(parse2)
val join2=data2.map(x=> ((x.account_Id),x))

val total_transactions2=data2.count()
val sum_of_transactions2=data2.map(_.amount).reduce(_+_)
val average_sys_2=sum_of_transactions2/total_transactions2
val output=join1.join(join2).map{case (s,(bank_sys1(a,b,c,d,e),bank_sys2(f,g,h,i,j,k)))=>(a,e+k)}
var sum_of_systems=output.map(_._2).sum()
var total_system_transactions=output.count()
var req_average=sum_of_systems/total_system_transactions

//year wise compute avg. of amount
var pair_year1=data1.map(x=>((x.transaction_time),x.amount)).groupByKey()
var pair_year2=data2.map(x=>((x.transaction_time),x.amount)).groupByKey()
var result_year1=pair_year1.map{case (key,value)=> (key,value.sum/value.size)}
var result_year2=pair_year2.map{case (key,value)=> (key,value.sum/value.size)}
var pair_result=result_year1.join(result_year2)
println("result")
var final_result=pair_result.map{case (a,(b,c))=>(f"year=$a%5s","avg="+(b+c)/2)}.foreach(println)

//balance
var rd=sc.textFile("file:///home/roshni/Downloads/project/bank/Transaction Sample data-1.csv")
var filtr1=rd.filter(x=> !x.contains("Account_id"))
val data11=filtr1.map(parser)
var rd1=sc.textFile("file:///home/roshni/Downloads/project/bank/Transaction Sample data-2.csv")
val fil1=rd1.filter(x=> !x.contains("Account_id"))
val data22=fil1.map(parset)
val res35=data11.union(data22)
var vv=res35.map(x=>((x.account_Id),x.amount)).groupByKey().map{case(key,value)=>("accuntId="+key,"balance= $"+value.sum)}.collect.toList
println(vv.max)


 sc.stop()

}
case class bank_sys1(account_Id:String,name:String,transaction_time:String,transaction_type:String,amount:Int)extends Serializable{}

def parse1(row:String):bank_sys1={
val field=row.split(",")
val account_Id=field(0)
val name=field(1)
val transaction_time=field(2).substring(field(2).length-4)
val transaction_type=field(3)
val regex=raw"\d+".r
val amount:Int=regex.findFirstMatchIn(field(4)).get.toString.toInt
bank_sys1(account_Id,name,transaction_time,transaction_type,amount)
}

case class bank_sys2(account_Id:String,name:String,transaction_mode:String,transaction_time:String,transaction_type:String,amount:Int)extends Serializable{}
def parse2(row:String):bank_sys2={
val field=row.split(",")
val account_Id=field(0)
val name=field(1)
val transaction_mode=field(2)
val transaction_time=field(3).substring(field(3).length-4)
val transaction_type=field(4)
val amount:Int=field(5).substring(1).replaceAll("\\s", "").toInt
bank_sys2(account_Id,name,transaction_mode,transaction_time,transaction_type,amount)
}

case class banka(account_Id:String,transaction_type:String,amount:Int)extends Serializable{}

def parser(row:String):banka={
val field=row.split(",")
val account_Id=field(0)
val transaction_type=field(3)
val regex=raw"\d+".r

var amount:Int=regex.findFirstMatchIn(field(4)).get.toString.toInt 
if(field(3)=="D") amount=amount * -1
banka(account_Id,transaction_type,amount)
}
def parset(row:String):banka={
val field=row.split(",")
val account_Id=field(0)
val transaction_type=field(4)
var amount:Int=field(5).substring(1).replaceAll("\\s", "").toInt
if(field(4)=="D") amount=amount * -1
banka(account_Id,transaction_type,amount)
}

}

