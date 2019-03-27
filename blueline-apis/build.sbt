name := "blueline-apis"
organization := "io.cjx.blueline"
version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"
lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)
// Change dependepcy scope to "provided" by : sbt -DprovidedDeps=true <task>
// Change dependepcy scope to "provided" by : sbt -DprovidedDeps=true <task>
val providedDeps = Option(System.getProperty("providedDeps")).getOrElse("false")

providedDeps match {
  case "true" => {
    println("providedDeps = true")
    libraryDependencies ++= providedDependencies.map(_ % "provided")
  }
  case "false" => {
    println("providedDeps = false")
    libraryDependencies ++= providedDependencies.map(_ % "compile")
  }
}
libraryDependencies ++= Seq(
)
unmanagedJars in Compile += file("lib/config-1.3.3-SNAPSHOT.jar")
dependencyOverrides += "com.google.guava" % "guava" % "15.0"

