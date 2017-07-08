package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object Graph extends PipelineStep {
	private var sparkContextObj: SparkContext = null
	def init(value: String) : PipelineStep = {
		this
	}

	def required(sparkContext: SparkContext, data: Any) = {
		sparkContextObj = sparkContext
	}

	def execute(tenantName: String) {
		ArticleConnections.createGraph(sparkContextObj, tenantName)
	}

	def results() : Any = {
	}
}
