package kiwi

import org.apache.spark.{SparkConf, SparkContext}
import java.io.{FileWriter, File}
import scala.io.Source
import com.google.common.io.Files
import org.specs2.mutable.Specification

class KnitterTest extends Specification {

  def setSparkContext(master: Option[String], args: List[String], jars: Seq[String] = Nil) : SparkContext = {
    val conf = new SparkConf().setAppName("Knitter Test").setJars(jars)
    for (m <- master) {
      conf.setMaster(m)
      conf.set("spark.executor.memory", "10g")
      conf.set("spark.driver.maxResultSize", "512M")
    }
    new SparkContext(conf)
  }

  "A Knitter" should {
    "Knit" in {
      val sparkContext = setSparkContext(
        master = Some("local"),
        args   = List()
      )

      Pretty.enableDebug()
      Pretty.setTestLoad()
      //Knitter.executePipeline(sparkContext, "Jagbani", Array("Knitter", "Jagbani",  "tasteV2"))
      Knitter.executePipeline(sparkContext, "HinduBusiness", Array("Knitter", "HinduBusiness",  "tasteV2"))
      //Knitter.executePipeline(sparkContext, "Gizmodo", Array("Knitter", "Gizmodo",  "tasteV2"))
      //Knitter.executePipeline(sparkContext, "Matribhumi", Array("Knitter", "Matribhumi",  "tasteV2"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari", "graph"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari",  "taste"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari",  "taste"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari", "count"))
      //Knitter.executePipeline(sparkContext, "PunjabKesari", Array("Knitter", "PunjabKesari", "sort"))
    }
  }
}
