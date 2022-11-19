import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}

import math.pow
import java.io._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.Nil.combinations


/*
object test
{
  // Main Method
  case class Country(index: String, co2: String,gdp: String)

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    ///val textRDD = sc.textFile("2010_co2_gdp.csv")
    //println(textRDD.foreach(println)

    var empRdd = textRDD.map {
      line =>
        val col = line.split(",")
        Country(col(0), col(1), col(2))
    }

    val empDF = empRdd.toDF()
    empDF.show()

    var df2 = empDF.withColumn("co2", empDF("co2").cast("int"))
    df2 = empDF.withColumn("gdp", empDF("gdp").cast("float"))
    df2.show()
    //df2.filter(df2("year") === "1960").show(true)
  }
}
*/



object test
{
  // Main Method
  case class Country(index: String, country: String, year: String, co2: String, gdp: String)
  case class Point(x: Double, y: Double) {
    def error_square_fun(other: Point): Double =
      pow(x - other.x, 2) + pow(y - other.y, 2)
  }


  def main(args: Array[String]): Unit = {

   var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val textRDD = sc.textFile("data_prepared.csv")
    //println(textRDD.foreach(println)

    var empRdd = textRDD.map {
      line =>
        val col = line.split(",")
        Country(col(0), col(1), col(2), col(3), col(4))
    }
    val empRddZipped = empRdd.zipWithIndex()
    empRdd = empRddZipped.filter(_._2 > 0).keys    // Elimino la prima riga (l'intestazione) dall'RDD

    val empDF = empRdd.toDF()
    //empDF.show()


    var df2 = empDF.withColumn("year", empDF("year").cast("int"))
    df2 = empDF.withColumn("co2", empDF("co2").cast("float"))
    df2 = empDF.withColumn("gdp", empDF("gdp").cast("float"))//df2.show(100)
    //df2.filter(df2("year") === "1960").show(true)

    var mapAnnoDF = Map[Int, DataFrame]()
    for (anno <- 1959 to 2013) mapAnnoDF += (anno -> df2.filter(df2("year") === anno))
    //mapAnnoDF(2003).show()

    val empRddProva = empRddZipped.filter(_._2 > 6).filter(_._2 < 16).keys // Creo un dataframe per le prove
    val empDFProva = empRddProva.toDF()
    empDFProva.show()

    //creo gli indici iniziali da 0 a len(df)
    var indici = List.range(0, empDFProva.count().toInt)
    //println(indici) List(0, 1, 2, 3, 4, 5, 6, 7, 8)

    var dizionario : List[Any] = indici

    //creo tutte le combinazioni possibili degli indici

    // println(empDFProva.rdd.take(1).last(3) , empDFProva.rdd.take(1).last(4))
    val col_co2 = empDFProva.select("co2").map(_.getString(0)).collectAsList.map(_.toDouble).toList
    val col_gdp = empDFProva.select("gdp").map(_.getString(0)).collectAsList.map(_.toDouble).toList
    val xy_zip = col_co2 zip col_gdp
    //println(xy_zip(3)._1)
    //println(xy_zip(3)._2)


    //var error_list : List[Double] = List()
    // METODO 1
    //combinazioni.foreach {
    //  error_list = error_list :+ distance(xy_zip,_,dizionario)
    //}

    //println(error_list)
    // METODO 2 (forse meglio per parallelizzare)
    for( a <- 1 to 2) {
      var combinazioni = indici.filter(_!=(-1)).combinations(2).toList
      //combinazioni = combinazioni.filter(element => element(0)!=(-1) && element(1)!=(-1))
      println("-----------------")
      println(combinazioni)
      println("-----------------")
      //mapping della lista di combinazioni con l'errore quadratico associato
      val error_list = combinazioni.map(distance(xy_zip, _, dizionario))

      println(combinazioni(error_list.indexOf(error_list.min)))
      println(indici)
      // Combinazione con l'errore minimo minore
      val coppia = combinazioni(error_list.indexOf(error_list.min))
      indici = indici.updated(coppia(0), -1) // List.updated(index, new_value)
      indici = indici.updated(coppia(1), -1)

      indici = indici :+ indici.length
      dizionario = dizionario :+ coppia

      println(indici)
      println(dizionario)
    }



  }



  def combine(in: List[Int]): IndexedSeq[List[Int]] =
    for {
      len <- 2 to 2
      combinations <- in combinations len
    } yield combinations

  //comb.map(_.filter(_._2!= 1))


  //comb.filter(element => element(0)!=1 || element(1)!=1)
  //println(combine(List(1, 2, 3, 4, 5)))

      // [-1, -1, 2, 3, 4,   5]  indici
      // [0, 1, 2, 3, 4, (0, 1)] dizionario
      //3 5
      // [-1, -1, 2, -1, 4, -1   ,  6  ]  indici
      // [0, 1, 2, 3, 4, (0, 1),(3,5)]

      /*
      var indici = List(1,2,3)
      val coppia = (2,3)

      indici = indici.updated(coppia._1 - 1, -1)  // List.updated(index, new_value)
      indici = indici.updated(coppia._2 - 1, -1)
       */

      // [0, 1, 2, 3, 4,   5]  indici
      // [0, 1, 2, 3, 4, (0, 1)] dizionario

     //for(combinazione <- combine(List(1, 2, 3, 4, 5))){



  def distance(dataFrame: List[(Double,Double)], points: List[Int], dizionario: List[Any]): Double ={
       //0 1 2 3 4   5     6   indici
       //0 1 2 3 4 (0,1) (3,5) dizionario
       //(0,1)  (0,2) ,.... (5,6) combinazioni
       //(3,5)-> 3,(0,1) -> 3,0,1 flat

       var all_x : List[Double] = List()
       var all_y : List[Double] = List()
       var X, Y : Double = 0
       val original = points

      val n = points.length
      for(i <- points){
        all_x = all_x :+ dataFrame(i)._1 //CO2
        all_y = all_y :+ dataFrame(i)._2 //GDP

        X = X + dataFrame(i)._1
        Y = Y + dataFrame(i)._2
      }

      X = X/points.length
      Y = Y/points.length
      var ptMedio = Point(X,Y)
      var error_square=0.0
      var point=Point(0.0,0.0)
      for (i <- 0 to n-1) {
        point = Point(all_x(i),all_y(i))
        error_square = error_square + ptMedio.error_square_fun(point)

      }
      error_square
      }


 /* def expand(points: List[Int], dizionario: List[Any], n:Int): List[Int] = {
    var points_extend= List()
    if (points.max<n)
  }*/

    /*
     val pt1 = Point(1, 2)  //qui bisogna prendere le coordinate dal df e non ho capito come si fa
     val pt2 = Point(3, 4)
     val x = (pt1.x + pt2.x) / 2
     val y = (pt1.y + pt2.y) / 2

     val ptMedio = Point(x,y)
     //val dist= ptMedio.distance(pt1)
     val dist1= pt1.distance(pt2)

     dist1
  */
       //println(dist,dist1)



  //} stavo iniziando il calcolo dell'errore

}