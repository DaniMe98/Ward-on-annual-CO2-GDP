import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import plotly.Plotly._
import plotly._
import plotly.element._
import plotly.layout._

import java.io._
import java.util.concurrent._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.math.pow


object test extends java.io.Serializable
{

  var original_lenght = 0
  val conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._


  // Main Method

  // Usata per strutturare le righe del csv in input
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
    //(3,5)-> 3,(0,1) -> 3,0,1 expand

    val points_new = expand(points, dizionario)
    val points_new_length = points_new.length

    //println("POINT NEW: " + points_new)
    //println("LENGTH DATAFRAME: "+ dataFrame.length)

    val all_x = points_new.map(dataFrame(_)._1)   //CO2
    val all_y = points_new.map(dataFrame(_)._2)   //CO2

    val ptMedio = Point(all_x.sum / points_new_length, all_y.sum / points_new_length)

    val error_square = (all_x zip all_y).map(punto => ptMedio.error_square_fun(Point(punto._1, punto._2))).sum

    error_square
  }

  def expand(points : List[Int], dizionario: List[List[Int]]): List[Int]  = {

    var expanded_points = points

    while(! (expanded_points.max < original_lenght))
      expanded_points = expanded_points.flatMap(dizionario(_))

    expanded_points
  }


  def graph(cluster: List[Int], dizionario: List[List[Int]], col_co2: List[Double], col_gdp: List[Double], year: Int,col_country: List[String]): File = {

    var data: List[Trace] = List()

    for (i <- cluster.indices) {
      val extractor = expand(List(cluster(i)), dizionario)    // Espande le radici dei cluster madre
      val trace = Scatter(
        extractor.map(col_co2(_)), //List(1, 2, 3, 4),
        extractor.map(col_gdp(_)), //List(10, 15, 13, 17),
        mode = ScatterMode(ScatterMode.Markers),
        text = extractor.map(col_country(_))
      )
      data = data :+ trace
    }

    val xaxis = Axis(
      title = "GDP"
    )

    val yaxis = Axis(
      title = "Co2"
    )

    val layout = Layout(
      title = "Ward Plot on CO2/GDP"
    ).withXaxis(xaxis).withYaxis(yaxis)

    Plotly.plot("ward_"+ year.toString +".html", data, layout, openInBrowser=false)
  }

  /////////////////////////////////////

  def ward(data: DataFrame): Unit = {

    SparkSession.builder
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val data_reindexed = data.withColumn("index", monotonically_increasing_id())    // Il DF ha gli indici discontinui, in questo modo gli indici diventano continui a partire dallo zero (0,1,2,...)
    //data_reindexed.show()

    original_lenght = data_reindexed.count().toInt

    val year = data_reindexed.select(col("year")).first.getInt(0)

    // forest = List(0, 1, 2, 3, 4, 5, 6, 7, 8 ... len(df))
    var forest: List[Int] = List.range(0, data_reindexed.count().toInt)

    // dizionario in cui sono salvate le combinazioni dei cluster
    var dizionario: List[List[Int]] = forest.map(List(_))

    // lista contenente i valori di co2
    val col_co2 = data_reindexed.select("co2").map(_.getDouble(0)).collectAsList.toList
    // lista contenente i valori di gdp
    val col_gdp = data_reindexed.select("gdp").map(_.getDouble(0)).collectAsList.toList
    // lista contenente i valori di country
    val col_country = data_reindexed.select("country").map(_.getString(0)).collectAsList.toList

    // zip di co2 e gdp
    val xy_zip = col_co2 zip col_gdp

    // APPLICAZIONE WARD
    println("--------------------INIZIO CALCOLO--------------------------")
    while(forest.count(_ > -1) > 1) {   // Finche' non terminano le possibili combinazioni

      // Creazione delle combinazioni con i valori del forest disponibili(!= -1)
      val combinazioni = forest.filter(_ != (- 1)).combinations(2).toList

      // Mapping della lista di combinazioni con l'errore quadratico associato
      val error_list = combinazioni.par.map(distance(xy_zip, _, dizionario))

      // Combinazione con l'errore minimo minore
      val coppia = combinazioni(error_list.indexOf(error_list.min))

      // Aggiornamento dei forest, eliminiamo i cluster appena uniti dal forest
      forest = forest.updated(coppia(0), -1) // List.updated(index, new_value)
      forest = forest.updated(coppia(1), -1)

      // Creo un nuovo slot nei forest
      forest = forest :+ forest.length

      // Aggiungo la combinazione trovata corrispondente al nuovo slot del forest
      dizionario = dizionario :+ coppia
    }

    val cluster = number_cluster(dizionario)
    graph(cluster, dizionario, col_co2, col_gdp, year, col_country)    // Creazione del grafico
    //csv(cluster, data_reindexed, dizionario, sc)                     // Creazione del csv
  }

  def number_cluster(dizionario: List[List[Int]]): List[Int] = {
    val last = dizionario.last(0)
    val out = last :: dizionario.drop(last + 1).flatten.filter(_ < last)
    out
    // last(0) = Primo elemento dell'ultima coppia del dizionario che verrà preso come cluster.
    // out =  - "concateno" last(0)
    //        - Droppo tutti i valori prima di last perchè mi interessano tutti quelli tra last(0) e last(1)
    //        - Flatten, mi serve per mettere tutti i valori delle tuple nel dizionario allo stesso livello
    //        - Filtro tutti i valori in modo che siano <last.
  }

  def main(args: Array[String]): Unit = {

    // Cancella tutti i grafici salvati precedentemente
    for {
      files <- Option(new File(".").listFiles)
      file <- files if file.getName.endsWith(".html")
    } file.delete()

/*
    val conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._*/






/*
    // Prende i dati in input
    val df_textRDD = sc.textFile("data_prepared.csv")

    // I dati vengono divisi in colonne secondo le virgole
    val df_columnedRDD = df_textRDD.map {
      line =>
        val col = line.split(",")
        Country(col(0), col(1), col(2), col(3), col(4))
    }

    var df = df_columnedRDD.toDF()
    df.show()
 */


    // OTTIENE IL DF DIRETTAMENTE DAL CSV
    var df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load("data_prepared.csv")
    df = df.withColumnRenamed("_c0", "index")
            .withColumnRenamed("_c1", "country")
            .withColumnRenamed("_c2", "year")
            .withColumnRenamed("_c3", "co2")
            .withColumnRenamed("_c4", "gdp")
    //df.show()


    // Change the column data type (from string)
    df = df.withColumn("year", df("year").cast("int"))
    df = df.withColumn("co2", df("co2").cast("double"))
    df = df.withColumn("gdp", df("gdp").cast("double"))

    val anni : List[Int] = List.range(1990, 2014)           // List.range(a, b) = from a to b-1

    val df_annuali = anni.map(anno => df.filter(df("year") === anno.toString).toDF())    // SI PUO' MIGLIORARE PARTIZIONANDO IL DF SENZA DOVERLO SCORRERE PER OGNI ANNO
    //val df_annuali = anni.map(anno => (df.filter(df("year") === anno.toString).rdd))

    //time(df_annuali.map(ward(_)))


    // PROVE DI PARALLELIZZAZIONE SUGLI ANNI  ==> si impallano
    //time(df_annuali.par.map(ward(_, sc)))
    //time(for (anno <- (1990 to 2013).par) ward(df.filter(df("year") === anno), sc))
    //time(for (k <- (0 to anni.length).par) ward(df_annuali(k),sc))








    // PROVIAMO L'USO DEGLI RDD   ==> errore IllegalAccess

    //val df_RDD = sc.parallelize(df_annuali)
    //println("RDD PRINT")
    //println(df_RDD)     //ParallelCollectionRDD[14] at parallelize at main.scala:254
    //df_RDD.foreach(println)
    //df_RDD.foreach(_.show())
    //df_RDD.map(_.show())
    //time(df_RDD.map(ward(_)))







    // Seq[RDD[DataFrame]
    var df2003_rdd = df.filter(df("year") === "2003").toDF()
    var df2009_rdd = df.filter(df("year") === "2009").toDF()
    val df_RDD_prova = sc.parallelize(Seq(df2003_rdd, df2009_rdd))
    //val df_RDD_prova = sc.parallelize(Seq(df, df))
    df_RDD_prova.first().show()
    //df_RDD_prova.map(_.show())
    //df_RDD_prova.foreach(println)
    //df_RDD_prova.collect().foreach(println)









    // Seq[RDD[RDD[Row]]
    val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df.rdd
    var df2003_rdd1 = df.filter(df("year") === "2003").rdd
    var df2009_rdd1 = df.filter(df("year") === "2009").rdd
    val df_RDD_prova1 = sc.parallelize(Seq(df2003_rdd1, df2009_rdd1))
    //df_RDD_prova1.foreach(println)
    //df_RDD_prova1.collect().foreach(println)
  }
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