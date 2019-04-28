import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkDataframeimport {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("DataframeScala")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder
      .appName("SparkSpellList")
      .getOrCreate()


    /** chemin vers le Dataset en JSON qui a été créé avec le crawler*/
    val path = "D:\\Downloads\\spell.json"
    /**création de la Dataframe spark à partir du JSON*/
    val spellsDF = spark.read.json(path)

    spellsDF.printSchema()
    spellsDF.createOrReplaceTempView("test")

    /**syntaxe longue mais obligatoire car les intitulés de classe ne sont pas normailsés sur d20pfsrd*/
    val usableSpells = spark.sql("SELECT `name`, `components` FROM test WHERE  ((test.levels.`sorcerer/wizard` = '0' OR\ntest.levels.`sorcerer/wizard` = '1' OR test.levels.`sorcerer/wizard` = '2' OR test.levels.`sorcerer/wizard` = '3' OR test.levels.`sorcerer/wizard` = '4') OR (test.levels.`sorcerer / wizard` = '0' OR test.levels.`sorcerer / wizard` = '1' OR test.levels.`sorcerer / wizard` = '2' OR test.levels.`sorcerer / wizard` = '3' OR test.levels.`sorcerer / wizard` = '4') OR (test.levels.`sorcerer/  wizard` = '0' OR test.levels.`sorcerer/  wizard` = '1' OR test.levels.`sorcerer/  wizard` = '2' OR test.levels.`sorcerer/  wizard` = '3' OR test.levels.`sorcerer/  wizard` = '4') OR  (test.levels.`sorcerer/ wizard` = '0' OR test.levels.`sorcerer/ wizard` = '1' OR test.levels.`sorcerer/ wizard` = '2' OR test.levels.`sorcerer/ wizard` = '3' OR test.levels.`sorcerer/ wizard` = '4')) AND NOT (array_contains(test.components, \"S\") OR array_contains(test.components, \"M\"))")
    //usableSpells.show()
    usableSpells.foreach(tup=>println(tup))
  }
}

