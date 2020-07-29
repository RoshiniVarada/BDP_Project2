import breeze.stats.mode
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._


object Object4 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c://winutils/")

    val sc = new SparkContext("local[*]", "SparkDemo")
    sc.setLogLevel("WARN")
    // Load the edges as a graph
    val spark = SparkSession.builder.appName("myA").getOrCreate()

    val groupEdgesDF = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("input/group-edges.csv")
    val groupMeta = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("input/meta-groups.csv")

    val verticesIDs = groupMeta.select("group_id", "group_name").withColumnRenamed("group_id", "id")
    val edgesColumns = groupEdgesDF.select("group1", "group2", "weight").withColumnRenamed("group1", "src")
      .withColumnRenamed("group2", "dst").withColumnRenamed("weight", "w")

    val myGraph = GraphFrame(verticesIDs, edgesColumns)

    // Run PageRank until convergence to tolerance "tol".
    val results = myGraph.pageRank.tol(0.0001).run()

    // Display resulting pageranks and final edge weights

    val VerResults = results.vertices.select("group_name", "pagerank")
    val EdgesResults = results.edges.select("src", "dst", "w", "weight")

    VerResults.orderBy(desc("pagerank")).show()
    EdgesResults.orderBy(desc("weight")).show()

    VerResults.orderBy(desc("pagerank")).coalesce(1)
      .write.mode(SaveMode.Overwrite).csv("output/VerticesResults")

    EdgesResults.orderBy(desc("weight")).coalesce(1)
      .write.mode(SaveMode.Overwrite).csv("output/EdgesResults")


  }
}
