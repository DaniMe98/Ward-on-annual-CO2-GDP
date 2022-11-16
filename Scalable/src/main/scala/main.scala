import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

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
    mapAnnoDF(2003).show()
  }
}