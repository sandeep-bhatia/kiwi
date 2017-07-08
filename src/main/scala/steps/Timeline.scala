package kiwi

import java.util._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext

object Timeline extends PipelineStep {

	//default to 48, but its just a placeholder
	private var daysBucket: String = "10"
	private var sparkContextObj: SparkContext = null
	private var buckets: ListBuffer[String] = null
	
	def init(value: String) : PipelineStep = {
		daysBucket = value
		this
	}

	def execute(tenantName: String) {
		buckets = ListBuffer[String]()
		val days = daysBucket.toInt

		if (Pretty.isTestLoad() == false) {
			for(offset <- 0 until days) {
				val suffix = getBucketSuffix(offset * -1)
				buckets += suffix
			}
		} else {
			buckets += "test/*"
		}
		Pretty.outputString("Buckets that need to be read are " +  buckets.mkString(","))
		buckets
	}
	
	def required(sparkContext: SparkContext, data: Any) {
		//there is no precursor to this execution
	}

	def getBucketSuffix(index: Int) = {
		val timeZone = TimeZone.getTimeZone("UTC")
		val calendar = Calendar.getInstance(timeZone)
		val year  = calendar.get(Calendar.YEAR);
		calendar.add(Calendar.HOUR_OF_DAY, index)
		val month = calendar.get(Calendar.MONTH) + 1; // Jan = 0, dec = 11
		val dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH); 
		val hourOfDay  = calendar.get(Calendar.HOUR_OF_DAY); 

		var yearStr = year.toString
		var monthStr = month.toString
		if (monthStr.length < 2) {
		  monthStr = "0" + monthStr
		}

		var dayOfMonthStr = dayOfMonth.toString
		if (dayOfMonthStr.length < 2) {
		  dayOfMonthStr = "0" + dayOfMonthStr
		}

		var hourOfDayStr = hourOfDay.toString
		if(hourOfDayStr.length < 2) {
		  hourOfDayStr = "0" + hourOfDayStr
		}

		val readBucket = yearStr + "/" + monthStr + "/" + dayOfMonthStr + "/" + hourOfDayStr + "/*"
		readBucket
  	}

  	def results() : Any = {
		buckets.toList
	}
}
