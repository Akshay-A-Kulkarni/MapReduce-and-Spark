package tc

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.broadcast


object TriangleJoins {
  // Creating a case class for defining edges.(DataSet)
  final case class Edge (
                          Follower :Int,
                          User     :Int
                        )

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 2) {
      logger.error("Usage:\nfc.ALL_JOINS <edge-file input> <JOIN_TYPE> <MAX_FILTER>")
      System.exit(1)
    }
    // Delete output directory, only to ease local development; will not work on AWS.
    // ================
    //        val hadoopConf = new org.apache.
    //        hadoop.conf.Configuration
    //        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //        try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    //    creating a SparkSession
    val spark = SparkSession.builder()
                             .master("local[*]")
                              .appName("TriangleJoins")
                               .getOrCreate()
    val sc = spark.sparkContext
    val MAX:Int = args(2).toInt;
    val joinType: String = args(1)

    joinType match {
                      case "RS_R"  => printResult(RS_R(args,sc,MAX))
                      case "RS_D"  => printResult(RS_D(args,spark,MAX))
                      case "REP_R" => printResult(REP_R(args,sc,MAX))
                      case "REP_D" => printResult(REP_D(args,spark,MAX))
                      // catch the default with a variable so you can print it
                      case _ => logger.error("Please choose a correct counting program")
    }
    def printResult(result: (Long, String)): Unit ={
      logger.info("\n********************************************\n"+
                  "          Triangle count :" +  result._1/3       +
                  "\n********************************************\n")
      logger.info(result._2)
    }

  }

  def RS_R(args: Array[String], sc: SparkContext, MAX:Int): (Long, String) = {

    val TC = sc.longAccumulator("TriangleCounter")
    val edges = sc.textFile(args(0)).map(edge => (edge.split(",")(0),edge.split(",")(1)))
      .filter(edges => edges._1.toInt <= MAX & edges._2.toInt <= MAX)
      .partitionBy(new HashPartitioner(1000)).cache()
    val edges2 = edges.map(edge => edge.swap)

    val trianglecount = edges2.join(edges).map(path => (path._2._2,path._2._1))
      .filter(path => path._1!=path._2)
      .join(edges)
    trianglecount.foreach(triangle => if(triangle._2._1==triangle._2._2){TC.add(1)})
    val debugString = trianglecount.toDebugString

    return Tuple2(TC.value,debugString)
  }

  def REP_R(args: Array[String], sc: SparkContext, MAX:Int): (Long, String)={

      val TC = sc.longAccumulator("TriangleCounter")
      val edges = sc.textFile(args(0)).map(edge => (edge.split(",")(0),edge.split(",")(1)))
                                      .filter(edges => edges._1.toInt <= MAX & edges._2.toInt <= MAX)
                                      .partitionBy(new HashPartitioner(1000))
                                      .cache()
      val edgeMap = sc.broadcast(edges.groupByKey().collectAsMap())
      val triangle = edges.map(edge => (edge._1,edgeMap.value.get(edge._2))) // getting the adjacency list for key node
                          .map(v2 => (v2._1,v2._2.toList.flatten)).flatMapValues(x=>x) // flattening the results
                          .filter(v2 => v2._1 != v2._2) // filtering out invalid 2-path loops
                          .map(v3 => edgeMap.value.get(v3._2).toList.flatten.contains(v3._1))
      triangle.foreach(t => if(t==true){TC.add(1)})
      val debugString = triangle.toDebugString

    return Tuple2(TC.value,debugString)
  }

    def RS_D(args: Array[String], spark: SparkSession, MAX:Int): (Long, String) ={
      import spark.implicits._ // importing implicits for type-casting
      val sc = spark.sparkContext
      val TC = sc.longAccumulator("TriangleCounter")
      val schema = Encoders.product[Edge].schema
      var edges = spark.read.option("header", "false")
        .schema(schema)
        .csv(args(0))
        .as[Edge].persist()
      if (MAX != -1) {
        edges = edges.filter($"Follower" <= MAX && $"User" <= MAX)
      }
      // Turning off auto-broadcast threshold to force broadcast on DataSet joins
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

      val join_expr1 = ($"DS1.User" === $"DS2.Follower" && $"DS1.Follower" =!= $"DS2.User")
      val join_expr2 = ($"DS2.User" === $"DS3.Follower")
      val triangle_cond = ($"DS3.User" === $"DS1.Follower")
      val triangle_counts = edges.as("DS1").join(edges.as("DS2"),join_expr1)
                                 .drop($"DS2.Follower").drop($"DS1.User")
                                 .join(edges.as("DS3"),join_expr2).filter(triangle_cond)
      triangle_counts.foreach(row => TC.add(1))
      val dfExplain = triangle_counts.explain(extended = true)
    return Tuple2(TC.value,dfExplain.toString)
  }

  def REP_D(args: Array[String], spark: SparkSession, MAX:Int): (Long, String)={
    import spark.implicits._ // importing implicits for type-casting
    val sc = spark.sparkContext
    val TC = sc.longAccumulator("TriangleCounter")
    val schema = Encoders.product[Edge].schema
    var edges = spark.read.option("header", "false")
                          .schema(schema)
                          .csv(args(0))
                          .as[Edge].persist()
    if (MAX != -1) {
      edges = edges.filter($"Follower" <= MAX && $"User" <= MAX)
    }
    // Turning off auto-broadcast threshold to force broadcast on dataset joins
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    val twopath_join_expr    = ($"DS1.User" === $"DS2.Follower" && $"DS1.Follower" =!= $"DS2.User")
    val triangle_join_expr    = ($"DS2.User" === $"DS3.Follower" && $"DS3.User" === $"DS1.Follower")
    val triangle_counts = edges.as("DS1")
                               .join(broadcast(edges.as("DS2")),twopath_join_expr)
                               .drop($"DS2.Follower").drop($"DS1.User")
                               .join(broadcast(edges.as("DS3")),triangle_join_expr)

    triangle_counts.foreach(t => TC.add(1))

    val dfExplain = triangle_counts.explain(extended = true)

    return Tuple2(TC.value,dfExplain.toString)
  }
}
