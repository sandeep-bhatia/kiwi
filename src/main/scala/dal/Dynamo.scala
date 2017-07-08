package kiwi
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.{AttributeUpdate, DynamoDB, Item}
import com.amazonaws.regions.Regions
import com.amazonaws.regions.Region
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable

object Dynamo {
 	
 	var dynamoConnection = setupDynamoClientConnection()

 	def setupDynamoClientConnection(): DynamoDB = {    
    	val awsCreds = new BasicAWSCredentials("AKIAJIQS5GRC32IWX3AA", "DgZV2drgqFbSXLlAoX9Cv9GsoxwC09IddFQXWJ1k")
		//val credentials = new EnvironmentVariableCredentialsProvider()
   		val client = new AmazonDynamoDBClient(awsCreds)
    	client.setRegion(Region.getRegion(Regions.US_WEST_2)); 
    	val dynamoDB = new DynamoDB(client)
      dynamoConnection = dynamoDB
    	dynamoDB
  	}

 	def setupHadoopConfiguration (sparkContext : SparkContext) {
    	val hadoopConf = sparkContext.hadoopConfiguration
    	hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    	hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIAJIQS5GRC32IWX3AA")
    	hadoopConf.set("fs.s3n.awsSecretAccessKey", "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")
  	}

  def readItemAttributeValue(sparkContext: SparkContext, tableName: String, keyName:String, key:String, attribute:String) : String = {
    try {
      val item = getItem(dynamoConnection, tableName, keyName, key)
      if (item == null) {
        ""
      } else {
          item.getJSON(attribute).replace("\"", "").replace("\\", "")
      }
    } catch {
       case e: Exception => {
          return ""
      }
    }
  }

  def timeNow(): Long = {
   System.currentTimeMillis()
  }

  def putItemStringValue(sparkContext: SparkContext, tableName:String, keyName: String, key:String, valueName: String, value:String) : Boolean = {
    try {  
      val table = dynamoConnection.getTable(tableName)
      var source = String.format("devo.%s", System.currentTimeMillis().toString)
      if (Pretty.isTestLoad() == false) {
        source = String.format("EMR.%s", System.currentTimeMillis().toString)
      } else {
        source = String.format("devo.%s", System.currentTimeMillis().toString)
      }

      val item = new Item().withPrimaryKey(keyName, key).withString(valueName, value).withNumber("updatedAt", timeNow()).withString("source", source)
      table.putItem(item)
      true
    } catch {
      case e: Exception => {
        Pretty.outputError("Failed to create item in dynamo db for recommendation" + tableName)
         Pretty.outputError("Failed to create item in dynamo db for recommendation" + key + "," + valueName + "," + value)
        Pretty.outputError(e.getMessage)
      }

      false
    }
  }

 	def readItemDataAsString(sparkContext: SparkContext, tableName: String, keyName:String, key:String) : String = {
 		getItemAsString(dynamoConnection, tableName, keyName, key)
 	}

 	def getItemAsString(dynamoDB: DynamoDB, tableName: String, keyName: String, key: String): String = {
   		val item = getItem(dynamoDB, tableName, keyName, key)

	    if(item != null) {
	      item.toString
	    } else {
	      null
	    }
  }

  def getItem(dynamoDB: DynamoDB, tableName: String, keyName: String, key: String): Item = {
      Pretty.outputString(String.format("table name: %s, key name: %s, key value: %s", tableName, keyName, key))
      val table = dynamoDB.getTable(tableName)
      val items = table.getItemOutcome(keyName, key)
      val item = items.getItem
      item
  }
}
