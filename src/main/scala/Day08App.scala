import org.apache.spark.sql.SparkSession

object Day08App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day08-DAGExecution").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val path = "sales.csv"
    val pw = new java.io.PrintWriter(path)
    pw.write("P1,North,100\nP2,South,200\nP1,South,150\nP3,North,300\nP2,North,250\n")
    pw.close()

    // Narrow transformations: map, filter (no shuffle, same-partition work)
    val raw = sc.textFile(path)
    val parsed = raw.map(_.split(","))                       // narrow
    val filtered = parsed.filter(a => a(2).toInt > 100)       // narrow

    // Wide transformation: reduceByKey (shuffle boundary -> new stage)
    val byRegion = filtered.map(a => (a(1), a(2).toInt)).reduceByKey(_ + _)  // wide

    // Another action after more narrow ops
    val result = byRegion.map { case (region, total) => s"$region: $total" }

    println("=== Execution plan (lineage) ===")
    println(result.toDebugString)
    // Look for "ShuffleRDD" or the indentation jump - that marks a new STAGE.
    // Everything above a shuffle boundary is one stage; everything below
    // (post-shuffle) is another. This job has 2 stages: one ending at the
    // reduceByKey shuffle-write, one starting at the shuffle-read.

    println("\n=== Result (this triggers the job -> stages -> tasks) ===")
    result.collect().foreach(println)

    // Jobs/Stages/Tasks/Partitions:
    // - Job = triggered by one action (here, collect())
    // - Stage = a set of transformations that can run without a shuffle
    // - Task = one stage applied to ONE partition (so #tasks per stage = #partitions)
    // - Partition = a chunk of the RDD's data

    spark.stop()
  }
}
