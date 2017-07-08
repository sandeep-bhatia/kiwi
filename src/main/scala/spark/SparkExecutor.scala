package kiwi

// Spark
import org.apache.spark.{SparkConf, SparkContext}

object SparkExecutor {

  def setSparkContext(master: Option[String], args: List[String], jars: Seq[String] = Nil) : SparkContext = {
    val conf = new SparkConf().setAppName("kiwi pipeline").setJars(jars)
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.cores", "16")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.akka.askTimeout", "600")
    conf.set("spark.akka.heartbeat.pauses", "12000")
    conf.set("spark.akka.heartbeat.interval", "6000")
    conf.set("spark.yarn.executor.memoryOverhead", "1000")
    conf.set("yarn.nodemanager.resource.cpu-vcores", "16")
    new SparkContext(conf)
  }

  def getStepOrder(stepKey: String, pipelineContext: String) : Int = {
    if (pipelineContext == "standard") {
      stepKey match {
      case "days" => 0
      case "s3bucket.read" => 1
      case "user"  => 2
      case "recommendation.write" => 3
      case "s3bucket.write" => 4
      }
    } else if (pipelineContext == "graph") {
      stepKey match {
      case "graph" => 0
      }
    } else if (pipelineContext == "sort") {
      stepKey match {
      case "sort" => 0
      case "recommendation.write" => 1
      }
    } else if (pipelineContext == "taste" || pipelineContext == "tasteV2") {
      stepKey match {
      case "days" => 0
      case "s3bucket.read" => 1
      case "historyCompute"  => 2
      case "taste" => 3
      case "recommendation.write" => 4
      case "merge" => 5
      case "recommendation.writeFinal" => 6
       case "s3bucket.write" => 7
      }
    } else if (pipelineContext == "count") {
      stepKey match {
      case "days" => 0
      case "s3bucket.read" => 1
      case "count"  => 2
      }
    }
    else {
      return 0
    } 
  }

  def executePipeline(sparkContext: SparkContext, pipelineDefinition: PipelineDefinition, tenantName: String, pipelineContext:String) {
  	val stepsUnordered = pipelineDefinition.executionSteps

    val stepLength = stepsUnordered.size.toInt
    Pretty.outputString(String.format("No of steps in execution of the pipeline %s", stepLength.toString))
    var steps = new Array[Tuple2[String, String]](stepLength)
    stepsUnordered.foreach(stepUnordered => {
      steps(getStepOrder(stepUnordered._1, pipelineContext)) = stepUnordered
    })
    
    Pretty.outputString(String.format("Executing pipeline %s with %s steps now", tenantName, stepLength.toString))
    var previousStep:Tuple2[String, String] = null

    //execute initial step
    steps.foreach(step => {
      
      if(previousStep != null) {
        pipelineDefinition.getExecutableStep(step).required(sparkContext, pipelineDefinition.getExecutableStep(previousStep).results)
      } else {
         pipelineDefinition.getExecutableStep(step).required(sparkContext, null)
      }

      Pretty.outputString(String.format("Executing pipeline steps %s", step._1.toString))
      pipelineDefinition.getExecutableStep(step).execute(tenantName) 
      Pretty.outputString(String.format("Execution complete pipeline steps %s", step._1.toString))
      previousStep = step
    })
  }
}
