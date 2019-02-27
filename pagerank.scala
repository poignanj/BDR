import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkPageRank {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("pagerankScala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()

    val links = sc.parallelize(List(("A",List("B","C")),("B", List("C")),("C",List("A")),("D", List("C")))).partitionBy(new HashPartitioner(4)).persist()
    val iters = 20

    var ranks = links.mapValues(v => 1.0)//met les rangs de départ sur les nodes
    var dampranks = links.mapValues(v=> 0.15)//valeur des arrivées externes
    println("---")

    for (i <- 1 to iters) {
      val contributions = links.join(ranks)
      val messages = contributions.flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank/links.size)) }
      val valMessages = messages.reduceByKey((x, y) => x + y)
      println("valeurs des contributions pour l'itération " + i)
      valMessages.foreach(s=> println(s))
      println(" ")
      var rankmodif = valMessages.mapValues(v => 0.85*v)//valeurs de redirection avec damping factor
      var dampedranks = rankmodif.union(dampranks)//union des redirections et des arrivées
      dampedranks = dampedranks.reduceByKey((x, y) => x + y)//nouveaux rangs prenant en compte le damping factor
      println("valeurs des poids pour l'itération " + i)
      dampedranks.foreach(s=> println(s))
      ranks = dampedranks.mapValues(v => v)
      println("--")
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2))

    spark.stop()
  }
}