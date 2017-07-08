import sbt._

object Dependencies {
  val resolutionRepos = Seq(
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Spray Repository" at "http://repo.spray.cc/"
  )

  object V {
    val spark     = "1.5.0"
    val specs2    = "1.13" // -> "1.13" when we bump to Scala 2.10.0
    val guava     = "11.0.1"
     // Java
    val awsSdk               = "1.9.34"
    val json4s               = "3.2.10"
    val argot                = "1.0.3"
    // Add versions for your additional libraries here...
  }

  object Libraries {
    val awsSdk       = "com.amazonaws"    % "aws-java-sdk"                 % V.awsSdk
    val awsSdkCore   = "com.amazonaws"    % "aws-java-sdk-core"            % V.awsSdk

    val sparkCore    = "org.apache.spark"           %% "spark-core"            % V.spark        % "provided"
    val sparkMllib   = "org.apache.spark"           %% "spark-mllib"           % V.spark        % "provided"
    val sparkSql     = "org.apache.spark"           %% "spark-sql"             % V.spark        % "provided"
    // Add additional libraries from mvnrepository.com (SBT syntax) here...

    // Scala (test only)
    val specs2       = "org.specs2"                 % "specs2_2.10"           % V.specs2       % "test"
    val guava        = "com.google.guava"           % "guava"                 % V.guava        % "test"

    val json4s       = "org.json4s"       %% "json4s-jackson"              % V.json4s
    val argot        = "org.clapper"      %% "argot"                       % V.argot
    val redshiftamazon = "com.amazon.redshift" %% "jdbc4" % "1.1.7.1007" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.1.7.1007.jar"
       
    val sparkredshift = "com.databricks" %% "spark-redshift" % "0.5.2"
  }
}
