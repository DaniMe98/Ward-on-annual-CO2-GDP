import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import plotly._
import plotly.element._
import plotly.layout._

import java.io._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.math.pow


object ProjectScalable {

  val path_GCP = "gs://my-bucket-scala/"      // Path iniziale del punto in cui si trovano i file in GoogleCloudPlatform (per il bucket "my-bucket-scala")

  val conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")    // If setMaster() value is set to local[*] it means the master is running in local with all the threads available
  //val conf = new SparkConf().setAppName("Read CSV File").setMaster("yarn")      // Su cloud
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

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

  def distance(dataFrame: List[(Double, Double)], points: List[Int], dizionario: List[List[Int]]): Double = {
    //0 1 2 3 4   5     6   forest
    //0 1 2 3 4 (0,1) (3,5) dizionario
    //(0,1)  (0,2) ,.... (5,6) combinazioni
    //(3,5)-> 3,(0,1) -> 3,0,1 expand

    val points_new = expand(points, dizionario,dataFrame.length)
    val points_new_length = points_new.length

    val all_x = points_new.map(dataFrame(_)._1)   //CO2
    val all_y = points_new.map(dataFrame(_)._2)   //GDP

    val ptMedio = Point(all_x.sum / points_new_length, all_y.sum / points_new_length)

    val error_square = (all_x zip all_y).map(punto => ptMedio.error_square_fun(Point(punto._1, punto._2))).sum

    error_square
  }

  def expand(points : List[Int], dizionario: List[List[Int]], original_lenght: Int): List[Int]  = {

    var expanded_points = points

    while(! (expanded_points.max < original_lenght))
      expanded_points = expanded_points.flatMap(dizionario(_))

    expanded_points
  }

  def graph(cluster: List[Int], dizionario: List[List[Int]], col_co2: List[Double], col_gdp: List[Double], year: Int, col_country: List[String]): File = {

    var data: List[Trace] = List()

    for (i <- cluster.indices) {

      val extractor = expand(List(cluster(i)), dizionario, col_co2.length)    // Espande le radici dei cluster madre

      val trace = Scatter()
        .withX(extractor.map(col_co2(_)))
        .withY(extractor.map(col_gdp(_)))
        .withMode(ScatterMode(ScatterMode.Markers))
        .withText(extractor.map(col_country(_)))

      data = data :+ trace
    }

    // Aggiungo la descrizione del grafico e le label degli assi
    val xaxis = Axis().withTitle("CO2")
    val yaxis = Axis().withTitle("GDP")
    val layout = Layout().withTitle("Ward Plot on CO2/GDP").withXaxis(xaxis).withYaxis(yaxis)

    Plotly.plot(path_GCP + "ward_" + year.toString + ".html", data, layout, openInBrowser=false)
  }

  def ward(length : Int, year : Int, col_co2 : List[Double], col_gdp : List[Double], col_country : List[String]): (List[Int], List[List[Int]], List[Double], List[Double], Int, List[String]) = {

    val original_lenght = length
    var forest: List[Int] = List.range(0, original_lenght)

    // dizionario in cui sono salvate le combinazioni dei cluster
    var dizionario: List[List[Int]] = forest.map(List(_))

    // zip di co2 e gdp
    val xy_zip = col_co2 zip col_gdp

    // APPLICAZIONE WARD
    println("--------------------INIZIO CALCOLO--------------------------"+original_lenght)
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

    //csv(cluster, data_reindexed, dizionario, sc)                     // Creazione del csv

    (cluster, dizionario, col_co2, col_gdp, year, col_country)
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
      files <- Option(new File(path_GCP + ".").listFiles)
      file <- files if file.getName.endsWith(".html")
    } file.delete()

    // Creazione df tramite il .csv
    var df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").load(path_GCP + "data_prepared.csv")     // Per GoogleCloudPlatform
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

    val df_annuali_reindexed = df_annuali.map(_.withColumn("index", monotonically_increasing_id()))

    val input_ward_annuali = df_annuali_reindexed.map(df_anno => (df_anno.count().toInt, df_anno.select(col("year")).first.getInt(0), df_anno.select("co2").map(_.getDouble(0)).collectAsList.toList, df_anno.select("gdp").map(_.getDouble(0)).collectAsList.toList, df_anno.select("country").map(_.getString(0)).collectAsList.toList))

    val RDD_inputWardAnnuali = sc.parallelize(input_ward_annuali)

    // VERSIONE DISTRIBUITA
    val t0 = System.nanoTime()
    val RDD_outputWardAnnuali = RDD_inputWardAnnuali.map(t => ward(t._1, t._2, t._3, t._4, t._5))
    RDD_outputWardAnnuali.collect().map(res => graph(res._1, res._2, res._3, res._4, res._5, res._6))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    //Elapsed time: 30s
/*
    // VERSIONE SEQUENZIALE
    val t0 = System.nanoTime()
    val outputWardAnnuali = input_ward_annuali.map(t => ward(t._1, t._2, t._3, t._4, t._5))
    outputWardAnnuali.map(res => graph(res._1, res._2, res._3, res._4, res._5, res._6))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    // Elapsed time: 51s
*/
/*
    // VERSIONE PARALLELA
    val t0 = System.nanoTime()
    val outputWardAnnuali = input_ward_annuali.par.map(t => ward(t._1, t._2, t._3, t._4, t._5))
    outputWardAnnuali.par.map(res => graph(res._1, res._2, res._3, res._4, res._5, res._6))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    //Elapsed time: 32s
 */
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