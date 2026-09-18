# Day 7 — Immutability, Lineage and Fault Tolerance

## Task
Build a multi-step RDD transformation chain, inspect its lineage, and explain how immutability and lineage together enable Spark's fault tolerance.

## Code — `src/main/scala/Day07App.scala`
```scala
import org.apache.spark.sql.SparkSession

object Day07App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day07-LineageFaultTolerance").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val path = "transactions.csv"
    val pw = new java.io.PrintWriter(path)
    pw.write("A1,100.0,North\nA2,250.5,South\nA3,-10.0,North\nA4,300.0,East\nA5,75.25,South\n")
    pw.close()

    val raw = sc.textFile(path)
    val parsed = raw.map(_.split(","))
    val valid = parsed.filter(arr => arr(1).toDouble > 0)
    val amounts = valid.map(arr => (arr(2), arr(1).toDouble))
    val totalsByRegion = amounts.reduceByKey(_ + _)

    println("=== Lineage (RDD dependency graph) ===")
    println(totalsByRegion.toDebugString)

    println("\n=== Result ===")
    totalsByRegion.collect().foreach(println)

    spark.stop()
  }
}
```

## Output
> Predicted — confirm by running `sbt run`. The exact `toDebugString` text varies slightly by Spark version, but the structure (indentation levels = stages) will match.
```
=== Lineage (RDD dependency graph) ===
(2) ShuffledRDD[5] at reduceByKey at Day07App.scala:16 []
 +-(2) MapPartitionsRDD[4] at map at Day07App.scala:15 []
    |  MapPartitionsRDD[3] at filter at Day07App.scala:14 []
    |  MapPartitionsRDD[2] at map at Day07App.scala:13 []
    |  transactions.csv MapPartitionsRDD[1] at textFile at Day07App.scala:12 []
    |  transactions.csv HadoopRDD[0] at textFile at Day07App.scala:12 []

=== Result ===
(North,100.0)
(South,325.75)
(East,300.0)
```

## Explanation — what's happening

**1. The transformation chain**
```
raw -> parsed -> valid -> amounts -> totalsByRegion
(read)  (split)  (filter>0) (pair)   (aggregate)
```
Each step produces a **new** RDD; none of the earlier RDDs (`raw`, `parsed`, `valid`) are ever modified. Note `A3,-10.0,North` gets dropped by the `filter(arr => arr(1).toDouble > 0)` step, so North's total (100.0) comes only from A1, not A3.

**2. `toDebugString` — reading the lineage**
The printed tree is Spark's own record of "how to rebuild `totalsByRegion` from scratch." Reading bottom-up: start from `HadoopRDD` (raw file read), through the `map`/`filter`/`map` chain (all narrow, hence flush against each other with `|`), until the `ShuffledRDD` at the top (from `reduceByKey`, marked with `+-` because it's a new stage boundary).

**3. Why immutability enables this**
Since `raw`, `parsed`, `valid`, and `amounts` are never mutated in place, each one is fully described by "my parent RDD + the operation I apply." Nothing about earlier RDDs can be corrupted or drift out of sync, because they're never touched again after being defined.

**4. Fault tolerance in practice**
If a partition of `totalsByRegion` were lost (e.g., an executor crashed), Spark doesn't recompute the entire job — it walks the lineage backward from the lost partition, finds exactly which upstream partition(s) of `amounts`/`valid`/`parsed`/`raw` fed it, and recomputes only that slice by re-reading the original file and re-applying the same `map`/`filter`/`map`/`reduceByKey` chain. Partitions that were never lost are left untouched.

## Viva Q&A
| Question | Answer |
|---|---|
| What does `toDebugString` actually show? | The RDD's lineage — a text representation of the DAG of parent RDDs and operations needed to rebuild it. |
| Why is there a `ShuffledRDD` at the top of the lineage? | `reduceByKey` is a wide transformation; it needs a shuffle to group matching keys onto the same partition, which Spark represents as a new RDD type (`ShuffledRDD`) and a new stage. |
| If an executor crashes mid-job, does Spark restart the whole computation? | No — it recomputes only the lost partition(s) by tracing back through the lineage to the original data source, leaving unaffected partitions alone. |
| Why does immutability matter for correctness here, not just performance? | Because no RDD is ever mutated, its lineage always accurately describes how to reproduce it — if RDDs could change in place, replaying the lineage could produce different results than the original run. |
| What happened to the `A3,-10.0,North` row? | It was removed by `filter(arr => arr(1).toDouble > 0)` since its amount is negative, so it never contributes to North's total. |
