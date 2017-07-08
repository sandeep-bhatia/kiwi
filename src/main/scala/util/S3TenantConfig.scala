package kiwi

import org.apache.spark.SparkContext

object S3TenantConfig extends DataMap {

	var RecommendationCount:Int = 50

	def readPipelineConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readPipelineConfig(sparkContext, tenantPipeline)		
	}

	def readCountConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readCountConfig(sparkContext, tenantPipeline)		
	}

	def readSortConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readSortConfig(sparkContext, tenantPipeline)		
	}

	def readTasteConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readTasteConfig(sparkContext, tenantPipeline)		
	}

	def readTasteV2Config(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readTasteV2Config(sparkContext, tenantPipeline)		
	}

	def readGraphConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		//read from s3 and give back the configuration
		S3.readGraphConfig(sparkContext, tenantPipeline)		
	}
}