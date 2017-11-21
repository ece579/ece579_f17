import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

object Regression{

	def main(args: Array[String]) {

	// Load and parse the data file
	val sc = new SparkContext("local", "Regression", "127.0.0.1", List("target/scala-2.11/regression_2.11-1.0.jar")) // Note: change according to your scala version 
	// Load and parse the data
        val spark_home = "" // Your path to Spark dir or $SPARK_HOME
	val data = sc.textFile(spark_home+"/data/mllib/ridge-data/lpsa.data")
	val parsedData = data.map { line =>
	  val parts = line.split(',')
	  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(x => x.toDouble).toArray))
	}

	// Building the model
	val numIterations = 20
	val model = LinearRegressionWithSGD.train(parsedData, numIterations)

	// Evaluate model on training examples and compute training error
	val valuesAndPreds = parsedData.map { point =>
	  val prediction = model.predict(point.features)
	  (point.label, prediction)
	}
	val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce(_ + _)/valuesAndPreds.count
	println("training Mean Squared Error = " + MSE)

}}
