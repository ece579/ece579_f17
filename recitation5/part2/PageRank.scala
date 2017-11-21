import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object Pagerank {
def main(args: Array[String]) {
val spark_home = "" // Change it according to your $SPARK_HOME
val sc = new SparkContext("local", "PageRank",
"127.0.0.1", List("target/scala-2.11/regression_2.11-1.0.jar")) // Change it according to your scala version
val graph = GraphLoader.edgeListFile(sc,spark_home+"/data/graphx/followers.txt")
// Run PageRank
val ranks = graph.pageRank(0.0001).vertices
// Join the ranks with the usernames
val users = sc.textFile(spark_home+"/data/graphx/users.txt").map { line =>
val fields = line.split(",")
(fields(0).toLong, fields(1))
}
val ranksByUsername = users.join(ranks).map {
case (id, (username, rank)) => (username, rank)
}
// Print the result
println(ranksByUsername.collect().mkString("\n"))
}
}
