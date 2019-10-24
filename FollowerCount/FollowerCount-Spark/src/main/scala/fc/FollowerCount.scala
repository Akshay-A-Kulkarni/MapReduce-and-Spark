package fc

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders


object FollowerCount {

  // Creating a case class for defining edges. (Dataset)
  final case class Edge (
                          Follower:Int,
                          User:Int
                        )

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 2) {
      logger.error("Usage:\nfc.FollowerCount <edge-file input> <output dir> <node file (optional)>")
      System.exit(1)
    }

    // Delete output directory, only to ease local development; will not work on AWS. ===========
        val hadoopConf = new org.apache.hadoop.conf.Configuration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    /*
        Spark 2.0.0 onwards, it is better to use sparkSession as it provides access to all the spark
        functionalities that sparkContext does. Also, it provides APIs to work on DataFrames and Datasets.
     */

    //    creating a SparkSession
    val spark = SparkSession.builder()
                            .master("local[*]")  // for local dev remove on aws
                            .appName("FollowerCount")
                            .getOrCreate()


    import spark.implicits._ // importing implicits for type-casting

    // creating endcoder and schema from case class
    val schema = Encoders.product[Edge].schema

    // loading the csv in a Spark DataSet
    val edges = spark.read.option("header", "false")
                          .schema(schema)
                          .csv(args(0))
                          .as[Edge]

    logger.debug(edges.explain()) // print physical plan to log

    // Single line code get follower counts
    val follower_counts = edges.groupBy("User").count()

    logger.debug(follower_counts.explain())

    if (args.length == 3){

      val nodes = spark.read.option("header", "false")
                            .option("inferSchema","True")
                            .csv(args(2)).toDF("User")

      // Join with null handling and sorting all in one

      nodes.join(follower_counts,Seq("User"),"leftouter")
           .sort(desc("Count")) // sorts in desc of user
           .na.fill(0,Seq("Count")) // replaces na with 0 for nodes without followers
           .write.format("csv").save(args(1))

    }
    else {
      // non-sorted output
      follower_counts.write.format("csv").save(args(1))

      // sorted version
//    follower_counts.sort(desc("Count")).write.format("csv").save(args(1))
    }
  }

}
