import sbt._
import Keys._

object WaterDropBuild extends Build {

  lazy val root = Project(id="waterdrop",
    base=file(".")) aggregate(apis, core) dependsOn(core)

  lazy val apis = Project(id="blueline-apis",
    base=file("blueline-apis"))

  lazy val core = Project(id="blueline-core",
    base=file("blueline-core")) dependsOn(apis)
}