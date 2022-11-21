import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, typedLit}

import math.pow
import java.io._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.Nil.combinations

object test
{

  var original_lenght = 0

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

    val empRddProva = empRddZipped.filter(_._2 > 0).filter(_._2 < 20).keys // Creo un dataframe per le prove
    val empDFProva = empRddProva.toDF()
    empDFProva.show()
    println(empDFProva)
    //creo gli indici iniziali da 0 a len(df)
    var indici : List[Int] = List.range(0, empDFProva.count().toInt)
    //println(indici) List(0, 1, 2, 3, 4, 5, 6, 7, 8)
    original_lenght=empDFProva.count().toInt
    //var dizionario : List[Any] = indici
    var dizionario : List[List[Int]] = indici.map(List(_))

    //creo tutte le combinazioni possibili degli indici

    // println(empDFProva.rdd.take(1).last(3) , empDFProva.rdd.take(1).last(4))
    val col_co2 = empDFProva.select("co2").map(_.getString(0)).collectAsList.map(_.toDouble).toList
    val col_gdp = empDFProva.select("gdp").map(_.getString(0)).collectAsList.map(_.toDouble).toList

    val xy_zip = col_co2 zip col_gdp

    println(xy_zip)
    //println(xy_zip(3)._1)
    //println(xy_zip(3)._2)


    //var error_list : List[Double] = List()
    // METODO 1
    //combinazioni.foreach {
    //  error_list = error_list :+ distance(xy_zip,_,dizionario)
    //}

    println(dizionario)
    // METODO 2 (forse meglio per parallelizzare)
    for( a <- 1 to original_lenght-1) {
      var combinazioni = indici.filter(_!=(-1)).combinations(2).toList
      //combinazioni = combinazioni.filter(element => element(0)!=(-1) && element(1)!=(-1))
      //NON ELIMINARE println("-----------------")
      //mapping della lista di combinazioni con l'errore quadratico associato
      val error_list = combinazioni.map(distance(xy_zip, _, dizionario))

      //NON ELIMINARE--------println("MSE tuple: ", combinazioni(error_list.indexOf(error_list.min)))

      // Combinazione con l'errore minimo minore
      val coppia = combinazioni(error_list.indexOf(error_list.min))
      indici = indici.updated(coppia(0), -1) // List.updated(index, new_value)
      indici = indici.updated(coppia(1), -1)

      indici = indici :+ indici.length
      dizionario = dizionario :+ coppia

      //NON ELIMINARE--------println("Index : ",indici)
    }


    println(dizionario)
    println("ULTIMO CLUSTER: "+ dizionario.last)


    for (i <- dizionario.last(0) to dizionario.length - 1) {
      println("CLUSTER "+ i + " -> " + dizionario(i))
    }
    var cluster: List[Int] = List()
    for(i <- dizionario.last(0) to dizionario.length-1) {
      println("---------------------------------------")
      println("CLUSTER NUMERO ->" + i)
      println("CLUSTER ->" + dizionario(i))
      // exp: è il cluster espanso attualmente analizzato
      var exp = expand(List(i), dizionario)
      println("CLUSTER ESPANSO -> " + exp)

      if(i == dizionario.last(0)){
        cluster = cluster :+ i
      }else {
        if (dizionario(i)(0) < dizionario.last(0)) {
          cluster = cluster :+ dizionario(i)(0)
        }
        if ((dizionario(i)(1) < dizionario.last(0))) {
          cluster = cluster :+ dizionario(i)(1)
        }
      }

    }
    println(cluster)
    println("NUMERO DI CLUSTER: "+ cluster.length)
    //Cluster: List(14, 6, 12)
    // Expand(14) -> 0, 4, 5, 1, 2, 3 -----> LABEL 1
    // Expand(6) ->  6                -----> LABEL 2
    // Expand(12) -> 7, 8             -----> LABEL 3

    var single_expanded: List[Int] = List()
    var label_expanded: List[Int] = List()
    var total_expanded: List[Product with scala.Serializable] = List()
    var a:List[(Int, Int)] = List()

    for (i <-0 to cluster.length-1){
      single_expanded = expand(List(cluster(i)),dizionario)
      label_expanded = List.fill[Int](single_expanded.length)(i)
      total_expanded = total_expanded :+  (single_expanded zip label_expanded)
    }
    //mergedList.seq.head._1.someField
    //total_expanded = total_expanded.sorted
   // total_expanded.seq.head._1._1




    // VERSIONE ALTERNATIVA "alla maniera di Scala"
    val cluster_expanded : List[List[Int]] = cluster.map(x => expand(List(x), dizionario))
    val cluster_zipped : List[List[(Int, Int)]] = cluster_expanded.zipWithIndex.map(x => x._1.zip(List.fill[Int](x._1.length)(x._2)))
    val cluster_flat : List[(Int, Int)] = cluster_zipped.flatten.sortBy(_._1)
    val label : List[Int] = cluster_flat.map(_._2)
    val label_DF = label.toDF()
    label_DF.show()
    val label_column = label_DF("value")
    println("DANIELE================", label_column)

    //label_DF.withColumn("num", List.range(1, original_lenght))

    //val df = empDFProva.withColumn("value", label_column.cast("int")) //empDFProva.withColumn("Label", empDFProva("gdp") + 1) // -- OK
   // val df2 = empDFProva.withColumn("value", typedLit(label.foreach {_})) }





   /* var i=0
    def customColumnVal( label_column: List[Int]):{
      var rd = label_column.asDict()
      rd["Label"]=label_column(i)
      i = i+1
      new_row=Row(**rd)
      return new_row
}

    //convert DF to RDD
      df_rdd = input_dataframe.rdd

    //apply new fucntion to rdd
    output_dataframe=df_rdd.map(customColumnVal).toDF()
      */



  }
    /*
    val df = empDFProva.withColumn("Label",lit("newValue"))//empDFProva.withColumn("Label", empDFProva("gdp") + 1) // -- OK
    df.show()
    */



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



  def distance(dataFrame: List[(Double,Double)], points: List[Int], dizionario: List[List[Int]]): Double ={
       //0 1 2 3 4   5     6   indici
       //0 1 2 3 4 (0,1) (3,5) dizionario
       //(0,1)  (0,2) ,.... (5,6) combinazioni
       //(3,5)-> 3,(0,1) -> 3,0,1 flat

      var all_x : List[Double] = List()
      var all_y : List[Double] = List()
      var X, Y : Double = 0
      var points_new= expand(points,dizionario)
      val n = points_new.length
      for(i <- points_new){

        all_x = all_x :+ dataFrame(i)._1 //CO2
        all_y = all_y :+ dataFrame(i)._2 //GDP


        X = X + dataFrame(i)._1
        Y = Y + dataFrame(i)._2
      }

      X = X/points_new.length
      Y = Y/points_new.length
      var ptMedio = Point(X,Y)
      var error_square=0.0
      var point=Point(0.0,0.0)
      for (i <- 0 to n-1) {
        point = Point(all_x(i),all_y(i))
        error_square = error_square + ptMedio.error_square_fun(point)
      }
      error_square
      }


  def expand(points: List[Int], dizionario: List[List[Int]]): List[Int] = {
    var points_extend : List[Int] = List()
    if (points.max<original_lenght){
      points
    }else{
      for(i <- points){ //points= List(1,6)   i=1  i=6
        if(dizionario(i).length > 1){
          var temp_list: List[Int] = dizionario(i)
          points_extend = points_extend:+ temp_list(0)
          points_extend = points_extend:+ temp_list(1)
          points_extend = expand(points_extend,dizionario)
        }else{
        points_extend = points_extend :+ i
        }

        /*
        // VERSIONE ALTERNATIVA "alla maniera di Scala"
        def aggiunta(i : Int) : List[Int] = dizionario(i) match {

          case single_num : Int => points_extend = points_extend :+ single_num //point.extend.append(i)
          case coppia : List[Int] => aggiunta(coppia(0))
        }
        */
      }
      points_extend
    }
  }
  /*
  // VERSIONE ALTERNATIVA "alla maniera di Scala" di expand
  def expand1(l : List[Any])  = {
    for (el <- l) {
      el match {
        case single_num : Int => points_extend = points_extend :+ single_num
        case coppia : List[Int] => expand1(coppia)
      }
    }
  }
  */


}