import sbt._
import Keys._

object KiwiBuild extends Build {

  import Dependencies._
  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("Kiwi", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        Libraries.sparkCore,
        Libraries.sparkMllib,
        Libraries.sparkSql,
        Libraries.guava,
        Libraries.specs2,
        Libraries.awsSdk,
        Libraries.awsSdkCore,
        Libraries.argot,
        Libraries.sparkredshift,
        Libraries.redshiftamazon
        // Add your additional libraries here (comma-separated)...
      )
    )
}
