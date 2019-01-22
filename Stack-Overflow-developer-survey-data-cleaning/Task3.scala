import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._
import org.apache.spark.{SparkConf, SparkContext}


object Task3 {
  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder().master("local").appName("Salary Countries").getOrCreate()
    import spark.implicits._
    val df = spark.read.option("header", true).csv(args(0))
    //val df = spark.read.option("header", true).csv("/Users/parth/Desktop/USC/Data Mining/Assignment1/survey_results_public.csv")
    df.createGlobalTempView("SurveyTable")

    val tempTable = spark.sql("SELECT Country, REPLACE(Salary,',','') as Salary, SalaryType FROM global_temp.SurveyTable WHERE (Salary != 'NA' AND Salary != '0')")
    //AND SalaryType != 'NA'
    tempTable.createGlobalTempView("tempTable")
    val tab1 = spark.sql("SELECT Country, CAST(Salary*12.0 as Double) as Salary FROM global_temp.tempTable WHERE (SalaryType == 'Monthly')")
    val tab2 = spark.sql("SELECT Country, CAST(Salary*52 as Double) as Salary FROM global_temp.tempTable WHERE (SalaryType == 'Weekly')")
    val tab3 = spark.sql("SELECT Country, CAST(Salary*1 as Double) as Salary FROM global_temp.tempTable WHERE (SalaryType == 'Yearly')")
    val tab4 = spark.sql("SELECT Country, CAST(Salary*1 as Double) as Salary FROM global_temp.tempTable WHERE (SalaryType == 'NA')")
    val finalDf = tab1.union(tab2).union(tab3).union(tab4)
    finalDf.createGlobalTempView("finalTable")
    val result = spark.sql("SELECT Country, COUNT(Salary) as NumberOFSalary, CAST(MIN(Salary) as int) as Min, CAST(MAX(Salary) as int) as Max, ROUND(CAST(AVG(Salary)as DOUBLE),2) as Average FROM global_temp.finalTable GROUP BY Country").orderBy("Country")

    //result.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save("/Users/parth/Desktop/USC/Data Mining/Assignment1/output1")
    result.coalesce(1).write.format("com.databricks.spark.csv").option("header","false").save(args(1))
  }
}
