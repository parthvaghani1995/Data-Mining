import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}




object Task1{
  def main(args:Array[String]) : Unit = {

    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val fs = FileSystem.get(sc.hadoopConfiguration)



    val spark = SparkSession.builder().master("local").appName("Salary Countries").getOrCreate()
    import spark.implicits._
    val df = spark.read.option("header", true).csv(args(0))
    //val df = spark.read.option("header", true).csv("/Users/parth/Desktop/USC/Data Mining/Assignment1/survey_results_public.csv")
    df.createGlobalTempView("SurveyTable")



    //df.filter(df("Salary").isNotNull).collect().foreach(println)
    //val result = spark.sql("SELECT * FROM global_temp.SurveyTable WHERE Salary != 'NA'").show()
    //val result1 = spark.sql("SELECT Respondent, Country, Salary FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0) ").sort("Country").show()
    //print("HELLO----------------------------------------------------")

    //val result = spark.sql("SELECT Country, Salary FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0)").groupBy("Country").count().orderBy("Country").coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save("/Users/parth/Desktop/USC/Data Mining/Assignment1/output")

    //val result = spark.sql("SELECT Country, Salary FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0)").groupBy("Country").count().orderBy("Country").coalesce(1).write.mode("append").csv("/Users/parth/Desktop/USC/Data Mining/Assignment1/output.csv")
    val result1 = spark.sql("SELECT Country, COUNT(Salary) as Salary FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != '0') GROUP BY Country ").orderBy("Country")
    var result2 = result1.createGlobalTempView("ABC")
    val result3 = spark.sql(("SELECT SUM(Salary) FROM global_temp.ABC ")).first()
    //val result4 = result3.getLong(0)
    val TotalDF = Seq(("Total", result3.getLong(0))).toDF()
    val FinalDF = TotalDF.union(result1)


    //val result2 = spark.sql("SELECT Country, COUNT(Salary) FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != 0) GROUP BY Country").sort("Country").coalesce(1)
    //FinalDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save("/Users/parth/Desktop/USC/Data Mining/Assignment1/output")
    //FinalDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save("/Users/parth/Desktop/USC/Data Mining/Assignment1/output")
    FinalDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save(args(1))

    // ---------- Renaming the file----------------
    //val file = fs.globStatus(new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/part*"))(0).getPath().getName()
    //println(file)
    //fs.rename(new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/" + file), new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/newData.csv"))
    /// ------------------------------------------

    //    var rdd = sc.textFile("/Users/parth/Desktop/USC/Data Mining/Assignment1/survey_results_public_test.csv")
    //    var header = rdd.first()
    //    var data = rdd.filter(row => row != header)
    //    var a = data.map(line => line.split(","))
    //    a.collect().foreach(arr=>arr.foreach(println))


    //header.take(1).foreach(println)
    //data.take(2).foreach(println)
    //print("HELLO")

  }
}
