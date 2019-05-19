
import org.apache.spark._
import scala.util.matching.Regex
import java.text.SimpleDateFormat
import java.util.Date

object Main extends App{

  override def main(arg:Array[String]) : Unit = {
    
 var sparkConf = new SparkConf().setMaster("local").setAppName("movies")
 var sc = new SparkContext(sparkConf)
 //rdd of movie dataset
 val Movie_rdd=sc.textFile("file:///home/roshni/Documents/ml-20m/movies.csv").filter(x =>(!x.contains("movieId")))
 val parse_movie=Movie_rdd.map(parseMovie)
 //rdd of ratings dataset
val data=sc.textFile("file:///home/roshni/Documents/ml-20m/ratings.csv")
var parse_rating=data.filter(x=> (!x.contains("movieId"))).map(parseRating)

//1-number of movies per year
val number_movies_perYear=parse_movie.map(x=>((x.year),x.movie_Id)).groupByKey().map{case (key,value)=>(f"year=$key%5s"," no of movies"+value.size)}
//1-number of ratings per year 
val number_ratings_perYear=parse_rating.map(x=>((x.year),x.rating)).groupByKey().map{case (key,value)=>(f"year=$key%5s"," no of ratedMovies= "+value.size)}

//2-per generes wise calculate number of movies
val Generes_no=parse_movie.map(x=>x.generes).collect.flatten.toSet.toList
 val array : Array[(String, org.apache.spark.rdd.RDD[movie])] = Array.ofDim(Generes_no.length)
 for(i<- 0 to Generes_no.length-1){
array(i)=(Generes_no(i), data.filter(x=>x.generes.contains(Generes_no(i))))
}
val MoviesCountPerGenre = array.map{ case (generes,movie_Id) => (generes,movie_Id.count)}.foreach(println)

//3-average ratings of movies
val avg_rating_movies=parse_rating.map(x=> ((x.movie_Id),x.rating)).groupByKey().map{case (key,value) => ("year="+key," avg.rating= "+value.reduce(_+_)/value.size)}.foreach(println)

//4-average ratings of all user 
val avg_user_rating=parse_rating.map(x=>(x.user_Id,x.rating)).groupByKey().map{case (key,value)=>("User="+key," avg.rating="+value.reduce(_+_)/value.size)}.foreach(println)

//average movie of particular movie in each year(taken movie1)
val pair_year_movie=parse_rating.map(x=>((x.year),x)).groupByKey()
val filter_movie_1=pair_year_movie.map{case (key,value)=> (key,value.filter(_.movie_Id==(1)))}
println("avg rating of movie 1 in all years")
val avg_movie_1=filter_movie_1.map{case(key,value)=>("year="+key,"movie 1 rating="+value.map(_.rating).reduce(_+_)/value.size)}.foreach(println)

}

//case class of movie csv.file
case class movie(movie_Id:Int,title:String,generes:Array[String],year:String) extends Serializable{}
def parseMovie(row:String):movie={
   val field=row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
   val movie_Id=field(0).toInt
   val title=field(1)
   val generes=field(2).split('|')
   val yearfit=field(1).trim()
   val length=yearfit.length()
   var string = yearfit.substring(length - 5,length).trim
   if(yearfit.charAt(length-1) == '"') string = yearfit.substring(length - 6,length-1).trim
   val year=string.substring(0,string.length-1)
   movie(movie_Id,title,generes,year)
}
case class ratings(user_Id:Int,movie_Id:Int,rating:Double,year:String) extends Serializable{}
def parseRating(row:String):ratings={
    val field=row.split(",")
    val user_Id=field(0).toInt
    val movie_Id=field(1).toInt
    val rating=field(2).toDouble
    val df:SimpleDateFormat=new SimpleDateFormat("yyyy")

    val year=df.format((field(3)+"000").toLong)
    
    ratings(user_Id,movie_Id,rating,year)
}

}

