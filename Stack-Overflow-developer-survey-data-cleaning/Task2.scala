import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}
import java.io.FileWriter




object Task2{
  def main(args:Array[String]) : Unit = {

    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fs = FileSystem.get(sc.hadoopConfiguration)



    val spark = SparkSession.builder().master("local").appName("Salary Countries").getOrCreate()
    import spark.implicits._
    //val df = spark.read.option("header", true).csv("/Users/parth/Desktop/USC/Data Mining/Assignment1/survey_results_public.csv")
    val df = spark.read.option("header", true).csv(args(0))
    df.createGlobalTempView("SurveyTable")





    val result1 = spark.sql("SELECT Country, Salary, SalaryType FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0)").select("Country").rdd.map(row => (row(0).toString, 1))
    var standpartition = result1.mapPartitions(iter => Array(iter.size.toString).iterator, true)
    val stand = standpartition.collect().toList.mkString(",")



    val result2 = spark.sql("SELECT Country, Salary, SalaryType FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0)").select("Country").repartition(df("Country")).repartition(2).rdd.map(row => (row(0).toString, 1))
    var specialPartition = result2.mapPartitions(iter => Array(iter.size).iterator, true)
    val special = specialPartition.collect().toList.mkString(",")

    var t1 = System.currentTimeMillis()
    result1.reduceByKey((a, b) => a + b).collect()

    var t2 = System.currentTimeMillis()
    result2.reduceByKey((a, b) => a + b).collect()

    var t3 = System.currentTimeMillis()


    //val f = new FileWriter("/Users/parth/Desktop/USC/Data Mining/Assignment1/output1.csv", true)
    val f = new FileWriter(args(1), true)
    f.write("standard," + stand + "," + (t2 - t1).toString() + "\n")
    f.write("partition," + special + "," + (t3 - t2).toString())
    f.close()


  }
}