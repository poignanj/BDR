import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


object SparkPageRank {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataframeScala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder
      .appName("MonsterMunch")
      .getOrCreate()


    /** chemin vers le Dataset en JSON qui a été créé avec le crawler*/
    val path = "C:\\Users\\reacher\\IdeaProjects\\osef\\src\\Bestiaire.json"
    val json = "C:\\Users\\reacher\\ext.json"
    /**création de la Dataframe spark à partir du JSON*/
    val MonsterDF = spark.read.json(path)

    MonsterDF.printSchema()


   val newDF = MonsterDF.select( MonsterDF("name"), explode(MonsterDF("spells")).alias("spell"))

    newDF.show()
    val DF3 = newDF.select(newDF("spell"),explode(newDF("name")).alias("name"))
    DF3.show()

    val rdd : RDD[(String, String)]= DF3.rdd.map (row => (row.getString(0), row.getString(1)))



    val res = rdd.reduceByKey((x, y) => (
      if(x contains y)
         x
      else
        x+ " | " + y
    ))
    res.cache()


    res.reduce((x,y) =>(
      if(x._1 == "heal")(
        x
      )
      else ("","")
    ))
    res.keys.foreach( x => (
      if((x contains "heal") || ((x contains "cure") && (x contains "wounds"))) println(x)
      ))
    res.collect().foreach(x => (
      if((x._1 contains "heal") || ((x._1 contains "cure") && (x._1 contains "wounds"))) println(x._1 + " ::: " + x._2)
      ))
  }
}