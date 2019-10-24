package tc

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._


object FollowerCounts {

  // Creating a case class for defining edges.(DataSet)
  final case class Edge (
                          Follower:Int,
                          User:Int
                        )

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 2) {
      logger.error("Usage:\nfc.FollowerCounts <edge-file input> <output dir> <node file (optional)>")
      System.exit(1)
    }

    //    creating a SparkSession
    val spark = SparkSession.builder()
      .master("local[*]") // for local dev remove on aws
      .appName("FollowerCounts")
      .getOrCreate()

    // Creating spark context for RDD's

    val sc = spark.sparkContext
    val textFile = sc.textFile(args(0))

    // Count type passed by user

    val countType: String = "RDD_R" +
      ""

    countType match {
      case "RDD_G" => logger.info(RDD_G(textFile).toDebugString)
      case "RDD_R" => logger.info(RDD_R(textFile).toDebugString)
      case "RDD_F" => logger.info(RDD_F(textFile).toDebugString)
      case "RDD_A" => logger.info(RDD_A(textFile).toDebugString)
      case "DSET" => logger.info(DSET(args,spark,1000).explain(extended = true))
      // catch the default with a variable so you can print it
      case _ => logger.error("Please choose a correct counting program")
    }


  }

  def RDD_G(textFile:RDD[String]):RDD[(String,Int)]={

    val counts = textFile.map(edge => (edge.split(",")(1),1))
      .groupByKey()
      .map(w =>(w._1,w._2.sum))

    return counts
  }

  def RDD_R(textFile:RDD[String]):RDD[(String,Int)]={

    val counts = textFile.map(edge => (edge.split(",")(1),1))
      .reduceByKey(_+_)

    return counts
  }

  def RDD_F(textFile:RDD[String]):RDD[(String,Int)]={

    val counts = textFile.map(edge => (edge.split(",")(1),1))
      .foldByKey(0)(_+_)

    return counts
  }

  def RDD_A(textFile:RDD[String]):RDD[(String,Int)]={

    val initialCount = 0;
    val addToCounts = (c1:Int, c2: Int) => c1 + c2  // within partition sum fn
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2 // between partition sum fn

    val counts = textFile.map(edge => (edge.split(",")(1),1))
      .aggregateByKey(initialCount)(addToCounts,sumPartitionCounts)

    /* shorter representation of above

    val counts = textFile.map(edge => (edge.split(",")(1),1))
      .aggregateByKey(0)(_+_,_+_)  */

    return counts
  }


  def DSET(args: Array[String],spark: SparkSession, MAX:Int):DataFrame={
    // creating encoder and schema from case class
    val schema = Encoders.product[Edge].schema
    // loading the csv in a Spark DataSet
    import spark.implicits._ // importing implicits for type-casting
    // creating a dataset directly from file.
    var edges = spark.read.option("header", "false")
                          .schema(schema)
                          .csv(args(0))
                          .as[Edge]
    if (MAX != -1) {
      edges = edges.filter($"Follower" <= MAX && $"User" <= MAX)
    }
    // Single line code get follower counts
    val follower_counts = edges.groupBy("User").count()

    return follower_counts
  }

}
