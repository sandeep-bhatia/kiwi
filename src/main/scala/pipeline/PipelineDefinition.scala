package kiwi

import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

class PipelineDefinition(sparkContext: SparkContext, tenantName: String, steps: scala.collection.Map[String, String]) {
  var tenant: String = tenantName
  var executionSteps: scala.collection.Map[String, String] = steps

  def stepNameToStepMap(key: String, value: String) : PipelineStep = {

    key match {
      case "days" => {
        val timeline = Timeline
        timeline.init(value)
        timeline
      }

      case "s3bucket.read" => {
        val s3Reader = S3Reader
        Pretty.outputString(String.format("bucket that will be read for the data %s", value))
        s3Reader.init(value)
        s3Reader
      }

      case "user"  => {
        val user = User
        user.init(value)
        user
      }
      
      case "merge" => {
        val merge = Merge
        Pretty.outputString("calling merge init with value " + value)
        merge.init(value)
        merge
      }

      case "s3bucket.write" => {
        val s3Writer = S3Writer
        s3Writer.init(value)
        s3Writer
      }

      case "count" => {
        val count = Count
        Pretty.outputString("calling count init with value " + value)
        count.init(value)
        count
      }

      case "taste" => {
        val taste = Taste
        Pretty.outputString("calling taste init with value " + value)
        taste.init(value)
        taste
      }

      case "historyCompute" => {
        val history = History
        Pretty.outputString("calling historyCompute init with value " + value)
        history.init(value)
        history
      }

      case "recommendation.write" => {
        val recommendations = Recommendation
        Pretty.outputString("calling recommendations init with value " + value)
        recommendations.init(value)
        recommendations
      }

      case "recommendation.writeFinal" => {
        val recommendations = Recommendation
        Pretty.outputString("calling recommendations init with value " + value)
        recommendations.init(value)
        recommendations
      }

      case "graph" => {
        val graph = Graph
        Pretty.outputString("calling graph init with value " + value)
        graph.init(value)
        graph
      }
    }
  } 

  def getExecutableStep(entry: Tuple2[String, String]) : PipelineStep = {
      stepNameToStepMap(entry._1, entry._2)
  }
}