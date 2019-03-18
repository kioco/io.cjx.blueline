package io.cjx.blueline.config
import java.util.ServiceLoader

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigRenderOptions}
import io.cjx.blueline.apis._
import org.antlr.v4.runtime.{ANTLRFileStream, CharStream, CommonTokenStream}
import io.cjx.blueline.configparser.{ConfigLexer, ConfigParser, ConfigVisitor}

import util.control.Breaks._

class ConfigBuilder(configFile: String) {
  val config = load()

  def load(): Config = {

    // val configFile = System.getProperty("config.path", "")
    if (configFile == "") {
      throw new ConfigRuntimeException("Please specify config file")
    }

    println("[INFO] Loading config file: " + configFile)

    // CharStreams is for Antlr4.7
    // val charStream: CharStream = CharStreams.fromFileName(configFile)
    val charStream: CharStream = new ANTLRFileStream(configFile)
    val lexer: ConfigLexer = new ConfigLexer(charStream)
    val tokens: CommonTokenStream = new CommonTokenStream(lexer)
    val parser: ConfigParser = new ConfigParser(tokens)

    val configContext: ConfigParser.ConfigContext = parser.config
    val visitor: ConfigVisitor[Config] = new ConfigVisitorImpl

    val parsedConfig = visitor.visit(configContext)

    val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
    System.out.println("[INFO] Parsed Config: \n" + parsedConfig.root().render(options))

    parsedConfig
  }


  def checkConfig: Unit = {
    val sparkConfig = this.getSparkConfigs
    val staticInput = this.createStaticInputs("batch")
    val streamingInputs = this.createStreamingInputs("streaming")
    val outputs = this.createOutputs[BaseOutput]("batch")
    val filters = this.createFilters
  }

  def getSparkConfigs: Config = {
    config.getConfig("spark")
  }

  def createFilters: List[BaseFilter] = {

    var filterList = List[BaseFilter]()
    config
      .getConfigList("filter")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "filter")

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[BaseFilter]

        obj.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))

        filterList = filterList :+ obj
      })

    filterList
  }

  def createStructuredStreamingInputs(engine: String): List[BaseStructuredStreamingInput] = {

    var inputList = List[BaseStructuredStreamingInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStructuredStreamingInput => {
            val input = inputObject.asInstanceOf[BaseStructuredStreamingInput]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStreamingInputs(engine: String): List[BaseStreamingInput[Any]] = {

    var inputList = List[BaseStreamingInput[Any]]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStreamingInput[Any] => {
            val input = inputObject.asInstanceOf[BaseStreamingInput[Any]]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createStaticInputs(engine: String): List[BaseStaticInput] = {

    var inputList = List[BaseStaticInput]()
    config
      .getConfigList("input")
      .foreach(plugin => {
        val className = buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "input", engine)

        val obj = Class
          .forName(className)
          .newInstance()

        obj match {
          case inputObject: BaseStaticInput => {
            val input = inputObject.asInstanceOf[BaseStaticInput]
            input.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))
            inputList = inputList :+ input
          }
          case _ => // do nothing
        }
      })

    inputList
  }

  def createOutputs[T <: Plugin](engine: String): List[T] = {

    var outputList = List[T]()
    config
      .getConfigList("output")
      .foreach(plugin => {

        val className = engine match {
          case "batch" | "sparkstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", "batch")
          case "structuredstreaming" =>
            buildClassFullQualifier(plugin.getString(ConfigBuilder.PluginNameKey), "output", engine)

        }

        val obj = Class
          .forName(className)
          .newInstance()
          .asInstanceOf[T]

        obj.setConfig(plugin.getConfig(ConfigBuilder.PluginParamsKey))

        outputList = outputList :+ obj
      })

    outputList
  }


  private def buildClassFullQualifier(name: String, classType: String): String = {
    buildClassFullQualifier(name, classType, "")
  }

  private def buildClassFullQualifier(name: String, classType: String, engine: String): String = {

    var qualifier = name
    if (qualifier.split("\\.").length == 1) {

      val inputType = name match {
        case _ if name.endsWith("Stream") => engine
        case _ => "batch"
      }
      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage + "." + inputType
        case "filter" => ConfigBuilder.FilterPackage
        case "output" => ConfigBuilder.OutputPackage + "." + engine
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala ++
          (ServiceLoader load classOf[BaseFilter]).asScala ++
          (ServiceLoader load classOf[BaseOutput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingInput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingOutput]).asScala ++
          (ServiceLoader load classOf[BaseStructuredStreamingOutputIntra])

      var classFound = false
      breakable {
        for (serviceInstance <- services) {
          val clz = serviceInstance.getClass
          // get class name prefixed by package name
          val clzNameLowercase = clz.getName.toLowerCase()
          val qualifierWithPackage = packageName + "." + qualifier
          if (clzNameLowercase == qualifierWithPackage.toLowerCase) {
            qualifier = clz.getName
            classFound = true
            break
          }
        }
      }
    }

    qualifier
  }
}

object ConfigBuilder {

  val PackagePrefix = "io.cjx.blueline"
  val FilterPackage = PackagePrefix + ".filter"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val PluginNameKey = "name"
  val PluginParamsKey = "entries"
}
