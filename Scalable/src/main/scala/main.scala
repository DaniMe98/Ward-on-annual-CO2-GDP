import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, scheduler}
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{lit, typedLit}
import plotly._
import element._
import layout._
import Plotly._
import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import math.pow
import java.io._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.Nil.combinations
import org.apache.spark.sql.functions._

import java.util.concurrent._
import scala.util.DynamicVariable


object test
{

  var original_lenght = 0

  // Main Method
  case class Country(index: String, country: String, year: String, co2: String, gdp: String)
  case class Point(x: Double, y: Double) {
    def error_square_fun(other: Point): Double =
      pow(other.x - x, 2) + pow(other.y - y, 2)
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
/*
  val scheduler = new DynamicVariable[TaskScheduler](new DefaultTaskScheduler)
  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    scheduler.value.parallel(taskA, taskB)
  }
*/
  def distance(dataFrame: List[(Double, Double)], points: List[Int], dizionario: List[List[Int]]): Double = {
    //0 1 2 3 4   5     6   forest
    //0 1 2 3 4 (0,1) (3,5) dizionario
    //(0,1)  (0,2) ,.... (5,6) combinazioni
    //(3,5)-> 3,(0,1) -> 3,0,1 flat
    var all_x: List[Double] = List()
    var all_y: List[Double] = List()
    var X, Y: Double = 0
    var points_new = expand(points, dizionario)
    val n = points_new.length
    for (i <- points_new) {
      all_x = all_x :+ dataFrame(i)._1 //CO2
      all_y = all_y :+ dataFrame(i)._2 //GDP

      X = X + dataFrame(i)._1
      Y = Y + dataFrame(i)._2
    }
    X = X / points_new.length
    Y = Y / points_new.length
    var ptMedio = Point(X, Y)
    var error_square = 0.0
    var point = Point(0.0, 0.0)
    for (i <- 0 to n - 1) {
      point = Point(all_x(i), all_y(i))
      error_square = error_square + ptMedio.error_square_fun(point)
    }
    error_square
  }
  ///////////////////////////////// EXPAND
  def expand(points: List[Int], dizionario: List[List[Int]]): List[Int] = {
    var points_extend: List[Int] = List()
    if (points.max < original_lenght) {
      points
    } else {
      for (i <- points) { //points= List(1,6)   i=1  i=6
        if (dizionario(i).length > 1) {
          var temp_list: List[Int] = dizionario(i)
          points_extend = points_extend :+ temp_list(0)
          points_extend = points_extend :+ temp_list(1)
          points_extend = expand(points_extend, dizionario)
        } else {
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
  ///////////////////////////////// EXPAND


  ///////////////////////////////////////////777
  def graph(cluster: List[Int], dizionario: List[List[Int]], col_co2: List[Double], col_gdp: List[Double], year: String): File = {
    var data: List[Trace] = List()

    for (i <- 0 to cluster.length - 1) {
      var extractor = expand(List(cluster(i)), dizionario)
      val trace = Scatter(
        extractor.map(col_co2(_)), //List(1, 2, 3, 4),
        extractor.map(col_gdp(_)), //List(10, 15, 13, 17),
        mode = ScatterMode(ScatterMode.Markers)
      )
      data = data :+ trace
    }
    val x = Layout(
      title = "x",
    )

    val layout = Layout(
      title = "Ward Plot on CO2/GDP"
    )//.withXaxis(xAxisOptions)

    Plotly.plot("ward_"+year+".html", data, layout)
  }
  /////////////////////////////////////

  def ward(empDFProva2: DataFrame, sc : SparkContext): Int = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var empDFProva = empDFProva2.withColumn("index", monotonicallyIncreasingId)
    empDFProva.show()
    // forest = List(0, 1, 2, 3, 4, 5, 6, 7, 8 ... len(df)
    var forest: List[Int] = List()
    forest = List.range(0, empDFProva.count().toInt)
    println("----------------------------------------------------")
    var year = empDFProva.select(col("year")).first.getString(0)
    println("----------------------------------------------------")
    original_lenght = empDFProva.count().toInt

    // dizionario in cui sono salvate le combinazioni dei cluster
    var dizionario: List[List[Int]] = forest.map(List(_))

    // lista contenente i valori di co2
    val col_co2 : List[Double] = empDFProva.select("co2").map(_.getString(0)).collectAsList.map(_.toDouble).toList
    // lista contenente i valori di gdp
    val col_gdp = empDFProva.select("gdp").map(_.getDouble(0)).collectAsList.toList
    // zip di co2 e gdp
    val xy_zip = col_co2 zip col_gdp

    println (xy_zip)
    println (dizionario)

    // APPLICAZIONE WARD
    for (counter <- 1 to original_lenght - 1) {
    // Creazione delle combinazioni con i valori del forest disponibili(!= -1)
    var combinazioni = forest.filter(_ != (- 1)).combinations(2).toList

    //mapping della lista di combinazioni con l'errore quadratico associato
    val error_list = combinazioni.map(distance(xy_zip, _, dizionario))

    // Combinazione con l'errore minimo minore
    val coppia = combinazioni(error_list.indexOf(error_list.min))

    // Aggiornamento dei forest, eliminiamo i cluster appena uniti dal forest
    forest = forest.updated (coppia (0), - 1) // List.updated(index, new_value)
    forest = forest.updated (coppia (1), - 1)
    // Creo un nuovo slot nei forest
    forest = forest :+ forest.length
    // Aggiungo la combinazione trovata corrispondente al nuovo slot del forest
    dizionario = dizionario :+ coppia
  }

    // Creazione del grafico
    var cluster = number_cluster(dizionario)
    //csv (cluster, empDFProva, dizionario, sc)
    graph (cluster, dizionario, col_co2, col_gdp, year)
    return 0
  }

  def number_cluster(dizionario: List[List[Int]]): List[Int] = {

    // NUMERO DI CLUSTER = TAGLIO DEL DENDOGRAMMA
    var cluster: List[Int] = List()
    //scorro tutti i valori presenti nel dizionario della root(ultimo cluster creato)
    for (i <- dizionario.last(0) to dizionario.length - 1) {
      //println("---------------------------------------")
      //println("CLUSTER NUMERO ->" + i)
      //println("CLUSTER ->" + dizionario(i))

      //Salvo nel primo cluster direttamente il valore del ramo più lungo del dendogramma
      if (i == dizionario.last(0)) {
        cluster = cluster :+ i
      } else {
        //Controllo che i cluster analizzati siano più piccoli del valore del ramo più lungo del dendogramma
        if (dizionario(i)(0) < dizionario.last(0)) {
          cluster = cluster :+ dizionario(i)(0)
        }
        if (dizionario(i)(1) < dizionario.last(0)) {
          cluster = cluster :+ dizionario(i)(1)
        }
      }
    }
    println("NUMERO DI CLUSTER: " + cluster.length)
    return cluster
  }

  /////////////////////////////////////////////////////////////////
  /*
  def csv (cluster: List[Int], empDFProva: DataFrame, dizionario: List[List[Int]], sc: SparkContext ): Unit = {
    import sqlContext.implicits._
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // UNIONE DEI CLUSTER LABEL AL DATAFRAME INIZIALE
    // Le radici dei cluster vengono espanse nei punti contenuti nel cluster
    val cluster_expanded: List[List[Int]] = cluster.map(x => expand(List(x), dizionario))
    // I punti dei cluster vengono associati con la label del cluster corrispondente
    val cluster_zipped: List[List[(Int, Int)]] = cluster_expanded.zipWithIndex.map(x => x._1.zip(List.fill[Int](x._1.length)(x._2)))
    // Le liste con i punti dei vari cluster vengono concatenate in un'unica lista e ordinate secondo l'ordine dei punti nel dataframe
    val cluster_flat: List[(Int, Int)] = cluster_zipped.flatten.sortBy(_._1)
    // Tengo soltanto le label associate ai punti (ordinate secondo l'ordinamento dei punti)
    val label: List[Int] = cluster_flat.map(_._2)
    // Aggiungo indici alle label per poter fare il join con il dataframe dei punti
    var label_indexed: DataFrame = label.zipWithIndex.toDF()
    label_indexed = label_indexed.withColumnRenamed("_1", "label").withColumnRenamed("_2", "id")
    // Aggiungo al dataframe dei punti una colonna con le label del cluster corrispondente
    var merged_df = empDFProva.join(label_indexed, empDFProva("index") === label_indexed("id"))
    merged_df = merged_df.drop("index").drop("country").drop("year").drop("id")
    //merged_df.show(100)

    // Salvo i dati del dataframe finale (co2 e gdp dei punti con label del cluster relativo)
    //merged_df.coalesce(1).write.option("header", "true").csv("output_csv")
  }
  */

/////////////////////////////////////////////////////////////////////////
  def main(args: Array[String]): Unit = {
    //cancella tutti i grafici salvati precedentemente
    for {
      files <- Option(new File(".").listFiles)
      file <- files if file.getName.endsWith(".html")
    } file.delete()

   var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val textRDD = sc.textFile("data_prepared.csv")

    var empRdd = textRDD.map {
      line =>
        val col = line.split(",")
        Country(col(0), col(1), col(2), col(3), col(4))
    }
    val empRddZipped = empRdd.zipWithIndex()
    empRdd = empRddZipped.filter(_._2 > 0).keys    // Elimino la prima riga (l'intestazione) dall'RDD

    val empDF = empRdd.toDF()


    var df2 = empDF.withColumn("year", empDF("year").cast("int"))
    df2 = empDF.withColumn("co2", empDF("co2").cast("double"))
    df2 = empDF.withColumn("gdp", empDF("gdp").cast("double"))//df2.show(100)
    //df2.filter(df2("year") === "1960").show(true)

    var mapAnnoDF = Map[Int, DataFrame]()
    for (anno <- 1990 to 2013) mapAnnoDF += (anno -> df2.filter(df2("year") === anno))
    //mapAnnoDF(2003).toDF().show()
    ////////
    val anni : List[Int] = List.range(1990, 1995)   // List.range(a, b) = from a to b-1
    val df_annuali : List[DataFrame] = anni.map(mapAnnoDF(_).toDF())
    df_annuali.map(ward(_, sc))

    //time(df_annuali.map(ward(_, sc)))   // senza parallellizare 48063ms
  }
}