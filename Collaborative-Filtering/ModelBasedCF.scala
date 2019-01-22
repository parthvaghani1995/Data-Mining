import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    val time1 = System.currentTimeMillis()

    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    ///Users/parth/Desktop/sample_movielens_data.txt
    //Users/parth/Downloads/hw2/Data/train_review.csv
    val data = sc.textFile("/Users/parth/Downloads/hw2/Data/train_review.csv")
    val testData = sc.textFile("/Users/parth/Downloads/hw2/Data/test_review.csv")
//    val data = sc.textFile(args(0))
//    val testData = sc.textFile(args(1))
    var userMap:Map[String,Int] = Map()
    var rUserMap:Map[Int,String] = Map()
    var businessMap:Map[String,Int] = Map()
    var rBusinessMap:Map[Int,String] = Map()

    print(data.collect().length)
    val headerAndRows = data.map(line => line.split(",").map(_.trim))
    val testHeaderAndRows = testData.map(line => line.split(",").map(_.trim))

    //Separate header
    val header = headerAndRows.first()
    val testHeader = testHeaderAndRows.first()

    //aSeparate header from data
    val dataNoHeader = headerAndRows.filter(_(0) != header(0))
    val testDataNoHeader = testHeaderAndRows.filter(_(0) != testHeader(0))

    val dataNoHeader1 = dataNoHeader.collect()
    //print(headerAndRows.first())
    //print(.collect().foreach(arr => println(arr.mkString(", "))))
    //print(dataNoHeader.take(5).foreach(arr => println(arr.mkString(", "))))

    var tempRating  = dataNoHeader.map(x => x).collect()
    var i = 1
    var j = 1
    //print(tempRating.foreach(println))
    for( x <- tempRating){
      var a = x(0)
      var b = x(1)
      if( !userMap.contains(x(0))){
        userMap += (x(0) -> i)
        rUserMap += (i -> x(0))
        x(0) = i.toString
        i = i + 1
      }

      if( !businessMap.contains(x(1))){
        businessMap += (x(1) -> j)
        rBusinessMap += (j -> x(1))
        j = j + 1
      }
    }
    //print(userMap)
    //print(tempRating.foreach(arr => println(arr.mkString(", "))))
    val ratings = dataNoHeader.map(x => Rating(userMap(x(0)).toInt, businessMap(x(1)).toInt, x(2).toDouble))

    //print(ratings.foreach(println))

    val rank = 3
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.265,1,1L)

    var tempuserData  = testDataNoHeader.map(x => x).collect()

    //print(tempuserData.foreach(arr => println(arr.mkString(", "))))
    for( x <- tempuserData){
      var a = x(0)
      var b = x(1)
      if( !userMap.contains(x(0))){
        userMap += (x(0) -> i)
        rUserMap += (i -> x(0))
        x(0) = i.toString
        i = i + 1
      }

      if( !businessMap.contains(x(1))){
        businessMap += (x(1) -> j)
        rBusinessMap += (j -> x(1))
        j = j + 1
      }
    }

    var userProducts1 = testDataNoHeader.map(x => Rating(userMap(x(0)).toInt, businessMap(x(1)).toInt, x(2).toDouble))

    val usersProducts = userProducts1.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        if (rate<0) ((user, product), 0.00000) else if (rate>5) ((user, product), 5.00000) else ((user, product), rate)
      }


    val output = predictions.map{ case((x, y), z) => (rUserMap(x),rBusinessMap(y), z)}

    val createFile = new PrintWriter(new File("Parth_Vaghani_ModelBasedCF.txt"))

    val tempOutput = output.sortBy( x => (x._1, x._2, x._3)).collect.toList

    for(line <- tempOutput){
      createFile.write(line._1+","+line._2+","+line._3+"\n")
    }

    createFile.close()
//    val writeToFile = predictions.map(x => (rUserMap(x._1._1) + "," + rBusinessMap(x._1._2) + "," + x._2)).coalesce(1)
//
//    val a = writeToFile.sortBy(x => x).collect().toList
//    print(a)
//


    //    val tempp = predictions.collect()
    //    val tempp1 = tempp.map match { case Array((user,business), rate) => rate}
    //
    //    print(tempp1)
    //print(predictions.collect().foreach(arr => println(arr)))

    // ---------- Renaming the file----------------
    //val file = fs.globStatus(new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/part*"))(0).getPath().getName()
    //println(file)
    //fs.rename(new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/" + file), new Path("/Users/parth/Desktop/USC/Data Mining/Assignment1/output/newData.csv"))
    /// ------------------------------------------


    val ratesAndPreds = userProducts1.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    //print(ratesAndPreds.collect().foreach(arr => println(arr.toString())))

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println(s"Mean Squared Error = $MSE")

    val temp =  userProducts1.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).count()

    val time2 = System.currentTimeMillis()
    //print(temp)
    val  RMSE=math.sqrt(MSE)
    val diff= ratesAndPreds.map { case ((user, product), (r1, r2)) => math.abs(r1 - r2)}

    var num1=0
    var num2=0
    var num3=0
    var num4=0
    var num5=0

    for (item <- diff.collect) {
      item match {
        case item if (item>=0 && item<1) => num1 = num1 + 1;
        case item if (item>=1 && item<2) => num2 = num2 + 1;
        case item if (item>=2 && item<3) => num3 = num3 + 1;
        case item if (item>=3 && item<4) => num4 = num4 + 1;
        case item if (item>=4 ) => num5 = num5 + 1;
      }
    }

    println(">=0 and <1:"+ num1)
    println(">=1 and <2:"+ num2)
    println(">=2 and <3:"+ num3)
    println(">=3 and <4:"+ num4)
    println(">=4 :"+ num5)
    println("RMSE = " + RMSE)
    println("Time :" + (time2 - time1)/1000 + "sec")



  }

}
