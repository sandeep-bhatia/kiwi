package kiwi
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object User extends PipelineStep {
	private var sparkContextObj: SparkContext = null
	private var rddUserArticle:RDD[Tuple2[String, String]] = null
	private var rank: Int = 8
	private var iterations: Int = 10
	private var lambda: Double = 0.01
	private var recommendationsCount: Int = 10
	private var recommendations:RDD[Tuple2[String, Array[String]]] = null

	def getRecommendationCount() : Int = {
		recommendationsCount
	}

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

	def readAlgorithmConfig() {
		val algorithmMap = S3.readAlgorithmConfig(sparkContextObj, "User")
		Pretty.outputMap(algorithmMap)
		rank = algorithmMap("rank").toInt
		iterations = algorithmMap("iterations").toInt
		lambda =  algorithmMap("lambda").toDouble
		recommendationsCount = algorithmMap("recommendations").toInt
		S3TenantConfig.RecommendationCount = recommendationsCount
	}

	def execute(tenantName: String) {
		readAlgorithmConfig()
		var ratings = rddUserArticle.map(record => {
			try {
					var rating = Rating(0, 0, 0)
					val article = record._2.replace("\"", "")
					val userId = record._1.replace("\"", "").toInt
					rating = Rating(userId.toInt, article.toInt , 1)
					rating
			}	catch {
				case e: Exception => {
					Pretty.outputError(e.getMessage)
					Rating(0, 0, 0)
				}
			}
		})

		val filteredSet = ratings.filter(entry => {
			entry.user > 0
		})

		if (Pretty.isTestLoad() == true) {
			val filtersCount = filteredSet.map { entry =>
				Pretty.outputString(String.format("filtered ratings %s, %s", entry.user.toString, entry.product.toString))
				entry
			}.count
		}
		
		val model = ALS.train(filteredSet, rank, iterations, lambda)
		val recommendationsOutput = model.recommendProductsForUsers(recommendationsCount).cache()
		if (Pretty.isTestLoad() == true) {

			val usersProducts = ratings.map { case Rating(user, product, rate) =>
				Pretty.outputString(String.format("ratings %s, %s,%s", user.toString, product.toString, rate.toString))
			 	(user, product)
			}

			val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => 
				String.format(String.format("Predicting the user product %s, %s, %s", user.toString, product.toString, rate.toString))
			   ((user, product), rate)
			}

			val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
			 ((user, product), rate)
			}.join(predictions)

			val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
			 val err = (r1 - r2)
			 err * err
			}.mean()
			println(String.format("Mean Squared Error = RANK: %s, ITERATIONS: %s, LAMBDA: %s, MSE : %s", rank.toString, iterations.toString, lambda.toString, MSE.toString))
		}

		recommendations = recommendationsOutput.map(userRating => {
				if (userRating._2 == null || userRating._2.length == 0) {
					(userRating._1.toString, Array[String]())
				} else {
			    	var productsRatings = userRating._2.map(currentRating => {
						currentRating.product.toString
			    	})
			    	(userRating._1.toString, productsRatings)
			    }
			
		}).cache()
	}

	def results() : Any = {
		recommendations
	}
}
