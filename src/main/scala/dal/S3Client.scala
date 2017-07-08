package kiwi
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.regions.Regions
import com.amazonaws.regions.Region
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.InputStream
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.GetObjectRequest

object S3Client {

  var s3Client = setupS3Client()
  var initialized = false;
 	def setupS3Client(): AmazonS3Client = {    
    val awsCreds = new BasicAWSCredentials("AKIAJIQS5GRC32IWX3AA", "DgZV2drgqFbSXLlAoX9Cv9GsoxwC09IddFQXWJ1k")
		//val credentials = new EnvironmentVariableCredentialsProvider()
   	val client = new AmazonS3Client(awsCreds)
    client.setRegion(Region.getRegion(Regions.US_WEST_2)); 
    client
  }

  def putStringObject(bucketName:String, keyName:String, contents:String) : Boolean = {
    try {
          if(initialized == false ) {
            initialized = true
            s3Client = setupS3Client()
          }
          val contentAsBytes = contents.getBytes("UTF-8");
          val contentsAsStream = new ByteArrayInputStream(contentAsBytes);
          val md = new ObjectMetadata();
          md.setContentLength(contentAsBytes.length);
          s3Client.putObject(new PutObjectRequest(bucketName, keyName, contentsAsStream, md));
          return true;
      } catch {
      case ase: AmazonServiceException => {
        Pretty.outputString("Caught an AmazonServiceException.");
        Pretty.outputString("Error Message:    " + ase.getMessage())
        Pretty.outputString("HTTP Status Code: " + ase.getStatusCode())
        Pretty.outputString("AWS Error Code:   " + ase.getErrorCode())
        Pretty.outputString("Error Type:       " + ase.getErrorType())
        Pretty.outputString("Request ID:       " + ase.getRequestId())
        false
      }
      case ace: AmazonClientException => {
        Pretty.outputString("Caught an AmazonClientException.");
        Pretty.outputString("Error Message: " + ace.getMessage());
        false
      }
    }
  }

  def getString(input: InputStream) : String = {
    // Read one text line at a time and display.
    val reader = new BufferedReader(new InputStreamReader(input));
    reader.readLine();
  } 

  def getStringObject(bucketName:String, keyName:String) : String = {
    try {
          if(initialized == false ) {
            initialized = true
            s3Client = setupS3Client()
          }
          Pretty.outputString(String.format("S3 read from the bucket: %s with key %s", bucketName, keyName))
          val item = s3Client.getObject(new GetObjectRequest(bucketName, keyName))
          getString(item.getObjectContent())
      } catch {
      case ase: AmazonServiceException => {
        Pretty.outputString("Caught an AmazonServiceException.");
        Pretty.outputString("Error Message:    " + ase.getMessage())
        Pretty.outputString("HTTP Status Code: " + ase.getStatusCode())
        Pretty.outputString("AWS Error Code:   " + ase.getErrorCode())
        Pretty.outputString("Error Type:       " + ase.getErrorType())
        Pretty.outputString("Request ID:       " + ase.getRequestId())
        null
      }
      case ace: AmazonClientException => {
        Pretty.outputString("Caught an AmazonClientException.");
        Pretty.outputString("Error Message: " + ace.getMessage());
        null
      }
    }
  }

 	def deleteObject(bucketName:String, keyName:String) {
 		try {
        if(initialized == false ) {
            initialized = true
            s3Client = setupS3Client()
        }
        s3Client.deleteObject(new DeleteObjectRequest(bucketName, keyName))
    } catch {
      case ase: AmazonServiceException => {
        Pretty.outputString("Caught an AmazonServiceException.");
        Pretty.outputString("Error Message:    " + ase.getMessage())
        Pretty.outputString("HTTP Status Code: " + ase.getStatusCode())
        Pretty.outputString("AWS Error Code:   " + ase.getErrorCode())
        Pretty.outputString("Error Type:       " + ase.getErrorType())
        Pretty.outputString("Request ID:       " + ase.getRequestId())
      }
      case ace: AmazonClientException => {
       Pretty.outputString("Caught an AmazonClientException.");
        Pretty.outputString("Error Message: " + ace.getMessage());
      }
    }
  } 
}
