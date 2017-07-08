package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SaveMode}



object Count extends PipelineStep {
	private var sparkContextObj: SparkContext = null
	private var rddUserArticle:RDD[Tuple2[String, String]] = null
	private var bucketedEventCounts:RDD[Tuple2[String, Int]] = null
	
	case class UserArticle(user:String, article:String)

	def init(value: String) : PipelineStep = {
		this
	}

	def required(sparkContext: SparkContext, data: Any) = {
		sparkContextObj = sparkContext
	
		data match {
  			case userArticleList: RDD[Tuple2[String, String]] => {
  				rddUserArticle = userArticleList
  			}
  			case _ => {
  				Pretty.outputString("Cast for required in S3 Reader failed") 
  				throw new ClassCastException
  			}
		}
	}

 	def removeQuotes(input: String): String = {
 		var output = "";
 		if (input.charAt(0) == '"') {
        	output = input.substring(1, input.length)
    	}
    	if(output.charAt(output.length - 1) == '"') {
    		output = output.substring(0, output.length - 1)
    	}
    	output
 	}

	def execute(tenantName: String) {
		S3.setupHadoopConfiguration(sparkContextObj)
		val articleIdHits = rddUserArticle.map(record => {
		try {
			val userId = removeQuotes(record._1)
			var articleId = removeQuotes(record._2).toLong
			(articleId.toString, 1)
		} catch {
			case e: Exception => {
				Pretty.outputString(String.format("error in record %s", record))
				Pretty.outputError(e.getMessage)
				("0", 1)
			}
		}}
		).cache();

		bucketedEventCounts = articleIdHits.reduceByKey((a, b) => {
			Pretty.outputString(String.format("reduction stage: %s, %s", a.toString, b.toString))
			a + b
		})

		// this is used to implicitly convert an RDD to a DataFrame.
		val sqlContext = new SQLContext(sparkContextObj)
		import sqlContext.implicits._

		val userarticles = rddUserArticle.map (userarticle => UserArticle(removeQuotes(userarticle._1), removeQuotes(userarticle._2))).toDF
		userarticles.write.option("url", "jdbc:redshift://datomata-pk-dw.c4j5plun24ws.us-west-2.redshift.amazonaws.com:5439/pkdw?user=pkuser&password=Punjabkesari123")
		.format("com.databricks.spark.redshift")
		.option("dbtable", "userarticles")
		.option("tempdir", "s3n://punjabkesari.temp/")
		.mode(SaveMode.Overwrite)
		.save()
		Pretty.outputString("Counting complete")
	}

	def results() : Any = {
		bucketedEventCounts
	}
}
