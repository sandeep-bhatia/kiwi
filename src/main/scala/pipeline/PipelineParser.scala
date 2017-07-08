package kiwi

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext

object PipelineParser {
	def parseDefinition(sparkContext: SparkContext, pipelineName: String, pipelineContext: String) : PipelineDefinition = {
		var s3TenantConfig:scala.collection.Map[String, String] = null

		if(pipelineContext == "standard") {
			s3TenantConfig = S3TenantConfig.readPipelineConfig(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		} else if (pipelineContext == "count") {
			s3TenantConfig = S3TenantConfig.readCountConfig(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the count configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		} else if (pipelineContext == "sort") {
			s3TenantConfig = S3TenantConfig.readSortConfig(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the sort configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		}
		else if (pipelineContext == "taste") {
			s3TenantConfig = S3TenantConfig.readTasteConfig(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the taste configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		}
		else if (pipelineContext == "graph") {
			s3TenantConfig = S3TenantConfig.readGraphConfig(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the taste configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		}
		else if (pipelineContext == "tasteV2") {
			s3TenantConfig = S3TenantConfig.readTasteV2Config(sparkContext, pipelineName)
			Pretty.outputString(String.format("Retrieved the taste configuration for the pipeline %s from S3 count: %s", pipelineName, s3TenantConfig.size.toString))
		}
		
		val pipelineDefinition = new PipelineDefinition(sparkContext, pipelineName, s3TenantConfig)
		pipelineDefinition
	}
}