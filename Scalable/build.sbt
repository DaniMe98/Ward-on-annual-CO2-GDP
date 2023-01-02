ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "Scalable"
  )



// Per l'utilizzo di Spark

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.plotly-scala" %% "plotly-render" %  "0.8.3"
)



// Per creare il file .jar
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "ProjectScalable.jar" }
