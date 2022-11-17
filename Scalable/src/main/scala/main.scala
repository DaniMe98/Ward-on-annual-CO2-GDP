import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import math.pow
import java.io._


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
    def distance(other: Point): Double =
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
    //lunghezza del dataframe per ottenere la lunghezza iniziale degli indici
    //println(empDFProva.count()) 9

    //creo gli indici iniziali da 0 a len(df)
    val indici = List.range(0, empDFProva.count().toInt)
    //println(indici) List(0, 1, 2, 3, 4, 5, 6, 7, 8)

    val dizionario = indici

    //creo tutte le combinazioni possibili degli indici
    val combinazioni = combine(indici)

    println(empDFProva.rdd.take(7).last(3) , empDFProva.rdd.take(7).last(4))

    // METODO 1
    combinazioni.foreach { println }
    // METODO 2 (forse meglio per parallelizzare)
    combinazioni.map(println(_))
  }




  def combine(in: List[Int]): IndexedSeq[List[Int]] =
    for {
      len <- 2 to 2
      combinations <- in combinations len
    } yield combinations

  //println(combine(List(1, 2, 3, 4, 5)))

      // [-1, -1, 2, 3, 4,   5]  indici
      // [0, 1, 2, 3, 4, (0, 1)] dizionario
      //3 5
      // [-1, -1, 2, -1, 4, -1   ,  6  ]  indici
      // [0, 1, 2, 3, 4, (0, 1),(3,5)]

  var indici = List(1,2,3)
  val coppia = (2,3)

  indici = indici.updated(coppia._1 - 1, -1)  // List.updated(index, new_value)
  indici = indici.updated(coppia._2 - 1, -1)

      // [0, 1, 2, 3, 4,   5]  indici
      // [0, 1, 2, 3, 4, (0, 1)] dizionario

     //for(combinazione <- combine(List(1, 2, 3, 4, 5))){



  def distance(dataFrame: DataFrame, points: List[Int], dizionario: List[List[Int]]): Double ={

       val all_x : List[Int] = List()
       val all_y : List[Int] = List()
       val original = points

      val n = points.length
      for(i <- points){
        all_x :+ dataFrame.rdd.take(i).last(3) //CO2
        all_y :+ dataFrame.rdd.take(i).last(4) //GDP
      }

      val X = all_x.sum / points.length    // .sum = Sommatoria
      val Y = all_y.sum / points.length



       val pt1 = Point(1, 2)  //qui bisogna prendere le coordinate dal df e non ho capito come si fa
       val pt2 = Point(3, 4)
       val x = (pt1.x + pt2.x) / 2
       val y = (pt1.y + pt2.y) / 2

       val ptMedio = Point(x,y)
       //val dist= ptMedio.distance(pt1)
       val dist1= pt1.distance(pt2)

       dist1
       //println(dist,dist1)
  }


  //} stavo iniziando il calcolo dell'errore

}