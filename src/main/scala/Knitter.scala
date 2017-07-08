package kiwi
import org.apache.spark.SparkContext

object Knitter {

  def validate(args: Array[String]) : Boolean = {
    var retVal: Boolean = false

    if (args.length > 1) {
      retVal = true
    }

    retVal
  }

  def main(args: Array[String]) {
  	Pretty.outputString("Knitter Created.")
    if (validate(args)) {
      //get the pipeline name
      Pretty.outputString(String.format("Processing pipeline %s .", args(1)))

      val sparkContext = SparkExecutor.setSparkContext(
        master    = None,
        args      = args.toList,
        jars      = List(SparkContext.jarOfObject(this).get)
      )

      executePipeline(sparkContext, args(1), args)
    }
  }
  
  def executePipeline(sparkContext:SparkContext, pipeline:String, args:Array[String]) {
    Pipeline.execute(sparkContext, pipeline, args)
  }
}