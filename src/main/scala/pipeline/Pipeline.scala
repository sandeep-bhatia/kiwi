package kiwi

import org.apache.spark.SparkContext

object Pipeline {
	private var context:String = "standard"
	def getPipelineContext() : String = {
		context
	}

	def execute(sparkContext:SparkContext, pipelineName: String, args: Array[String]) {
		var pipelineContext:String = "standard"
		args.foreach(arg => {
			
			if (arg == "count") {
				pipelineContext = "count"
			}

			if(arg == "sort") {
				pipelineContext = "sort"
			}

			if(arg == "taste") {
				pipelineContext = "taste"
			}

			if(arg == "tasteV2") {
				pipelineContext = "tasteV2"
			}

			if(arg == "graph") {
				pipelineContext = "graph"
			}
		})
		context = pipelineContext
    	val pipelineDefinition = PipelineParser.parseDefinition(sparkContext, pipelineName, pipelineContext)
    	SparkExecutor.executePipeline(sparkContext, pipelineDefinition, pipelineName, pipelineContext)
	}
}