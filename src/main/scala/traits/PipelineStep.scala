package kiwi
import org.apache.spark.SparkContext

trait PipelineStep {
	def init(value: String) : PipelineStep
	def execute(tenantName: String) 
	def results() : Any
	def required(sparkContext: SparkContext, data: Any)
}
