package kiwi
import org.apache.spark.SparkContext

trait DataMap {
	def readPipelineConfig(sparkContext: SparkContext, context: String) : scala.collection.Map[String, String]
}
