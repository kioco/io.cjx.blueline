package io.cjx.blueline

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigResolveOptions}
import io.cjx.blueline.StreamingPipleline.StreamingPipeline.{Batch, PipelineType, Streaming, Unknown}
import io.cjx.blueline.StreamingPipleline.{StreamingPipeline, StreamingPipelineBuilder, StreamingPipelineRunner}
import io.cjx.blueline.apis._
import io.cjx.blueline.config._
import io.cjx.blueline.filter.UdfRegister
import io.cjx.blueline.utils.{AsciiArt, CompressionUtils}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.streaming._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object BluelineSource {
  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)

        val configFilePath = Common.getDeployMode match {
          case Some(m) => {
            if (m.equals("cluster")) {
              // only keep filename in cluster mode
              new Path(cmdArgs.configFile).getName
            } else {
              cmdArgs.configFile
            }
          }
        }
        val appType = cmdArgs.engine

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {

            Try(entrypoint(configFilePath, appType)) match {
              case Success(_) => {}
              case Failure(exception) => {
                exception.isInstanceOf[ConfigRuntimeException] match {
                  case true => {
                    showConfigError(exception)
                  }
                  case false => {
                    showFatalError(exception)
                  }
                }
              }
            }
          }
        }
      }
      case None =>
      }
    }
  private def entrypoint(configFile: String, engine: String): Unit = {
    engine match {
      case "sparkstreaming" => {
        //val staticInputs = configBuilder.createStaticInputs(engine)
        //val streamingInputs = configBuilder.createStreamingInputs(engine)
        //val filters = configBuilder.createFilters
        //val outputs = configBuilder.createOutputs[BaseOutput](engine)

        //baseCheckConfig(staticInputs, streamingInputs, filters, outputs)
        //streamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)
        //----------
        //val rootConfig = ConfigFactory.parseFile(new File(configFile))

        if (configFile == "") {
          throw new ConfigRuntimeException("Please specify config file")
        }

        println("[INFO] Loading config file: " + configFile)

        // variables substitution / variables resolution order:
        // onfig file --> syste environment --> java properties
        val config = ConfigFactory
          .parseFile(new File(configFile))
          .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
          .resolveWith(ConfigFactory.systemProperties, ConfigResolveOptions.defaults.setAllowUnresolved(true))

        val options: ConfigRenderOptions = ConfigRenderOptions.concise.setFormatted(true)
        println("[INFO] parsed config file: " + config.root().render(options))
        val (rootPipeline, rootPipelineType, _) = StreamingPipelineBuilder.recursiveBuilder(config, "ROOT_PIPELINE")
        rootPipelineType match {
          case Unknown => {
            throw new ConfigRuntimeException("Cannot not detect pipeline type, please check your config")
          }
          case _ => {}
        }
        StreamingPipelineBuilder.validatePipeline(rootPipeline)
        StreamingPipelineBuilder.checkConfigRecursively(rootPipeline)
        Common.getDeployMode match {
          case Some(m) => {
            if (m.equals("cluster")) {

              println("preparing cluster mode work dir files...")

              // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
              val workDir = new File(".")
              println("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

              workDir.listFiles().foreach(f => println("\t list file: " + f.getAbsolutePath))

              // decompress plugin dir
              val compressedFile = new File("plugins.tar.gz")

              Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
                case Success(tempFile) => {
                  Try(CompressionUtils.unTar(tempFile, workDir)) match {
                    case Success(_) => println("succeeded to decompress plugins.tar.gz")
                    case Failure(ex) => {
                      println("failed to decompress plugins.tar.gz", ex)
                      sys.exit(-1)
                    }
                  }

                }
                case Failure(ex) => {
                  println("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }
            }
          }
        }

        streamingProcessing(config, rootPipeline, rootPipelineType)
      }

      case "structuredstreaming" => {
        val configBuilder = new ConfigBuilder(configFile)
        println("[INFO] loading SparkConf: ")
        val sparkConf = createSparkConf(configBuilder)
        sparkConf.getAll.foreach(entry => {
          val (key, value) = entry
          println("\t" + key + " => " + value)
        })
        val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
        UdfRegister.findAndRegisterUdfs(sparkSession)
        val staticInputs = configBuilder.createStaticInputs(engine)
        val streamingInputs = configBuilder.createStructuredStreamingInputs(engine)
        val filters = configBuilder.createFilters
        val outputs = configBuilder.createOutputs[BaseStructuredStreamingOutputIntra](engine)
        baseCheckConfig(staticInputs, streamingInputs, filters, outputs)
        structuredStreamingProcessing(sparkSession, configBuilder, staticInputs, streamingInputs, filters, outputs)
      }

      case "batch" => {
        val configBuilder = new ConfigBuilder(configFile)
        println("[INFO] loading SparkConf: ")
        val sparkConf = createSparkConf(configBuilder)
        sparkConf.getAll.foreach(entry => {
          val (key, value) = entry
          println("\t" + key + " => " + value)
        })

        val inputs = configBuilder.createStaticInputs(engine)
        val filters = configBuilder.createFilters
        val outputs = configBuilder.createOutputs[BaseOutput](engine)
        baseCheckConfig(inputs, filters, outputs)
        //batchProcessing(sparkSession, configBuilder, inputs, filters, outputs)
      }

      case _ => println("Unknown application type: " + engine)
    }
  }
  private def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()
    configBuilder.getSparkConfigs
      .entrySet().foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
  private def createPipleSparkConf(sparkConfig: Config): SparkConf = {
    val sparkConf = new SparkConf()

    sparkConfig
      .entrySet()
      .foreach(entry => {
        val key = entry.getKey
        val value = String.valueOf(entry.getValue.unwrapped())
        println("spark config - key: %s, value: %s".format(entry.getKey, value))
        sparkConf.set(key, value)
      })

    sparkConf
  }
  private def showWaterdropAsciiLogo(): Unit = {
    AsciiArt.printAsciiArt("blueline")
  }

  private def showConfigError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Config Error:\n")
    println("Reason: " + errorMsg + "\n")
    println("\n===============================================================================\n\n\n")
  }

  private def showFatalError(throwable: Throwable): Unit = {
    println("\n\n===============================================================================\n\n")
    val errorMsg = throwable.getMessage
    println("Fatal Error, \n")
    println(
      "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n")
    println("Reason: " + errorMsg + "\n")
    println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable))
    println("\n===============================================================================\n\n\n")
  }
  def batchProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    filters: List[BaseFilter],
    outputs: List[BaseOutput]): Unit = {
    basePrepare(sparkSession, staticInputs, filters, outputs)

    // let static input register as table for later use if needed
    registerTempView(staticInputs, sparkSession)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    if (staticInputs.nonEmpty) {
      var ds = staticInputs.head.getDataset(sparkSession)

      for (f <- filters) {
        if (ds.take(1).length > 0) {
          ds = f.process(sparkSession, ds)
        }
      }
      outputs.foreach(p => {
        p.process(ds)
      })

    } else {
      throw new ConfigRuntimeException("Input must be configured at least once.")
    }
  }
  private def structuredStreamingProcessing(
    sparkSession: SparkSession,
    configBuilder: ConfigBuilder,
    staticInputs: List[BaseStaticInput],
    structuredStreamingInputs: List[BaseStructuredStreamingInput],
    filters: List[BaseFilter],
    structuredStreamingOutputs: List[BaseStructuredStreamingOutputIntra]): Unit = {
    basePrepare(sparkSession, staticInputs, structuredStreamingInputs, filters, structuredStreamingOutputs)
    //监控
    //listener(sparkSession)
    val datasetList = structuredStreamingInputs.map(p => {
      p.getDataset(sparkSession)
    })

    // let static input register as table for later use if needed
    registerTempView(staticInputs, sparkSession)

    showWaterdropAsciiLogo()

    var ds: Dataset[Row] = datasetList.get(0)
    for (f <- filters) {
      ds = f.process(sparkSession, ds)
    }

    var streamingQueryList = List[StreamingQuery]()

    for (output <- structuredStreamingOutputs) {
      val start = output.process(ds).start()
      streamingQueryList = streamingQueryList :+ start
    }

    for (streamingQuery <- streamingQueryList) {
      streamingQuery.awaitTermination()
    }
  }
  private def streamingProcessing(rootConfig: Config, rootPipeline: StreamingPipeline, rootPipelineType: PipelineType): Unit = {
    println("[INFO] loading SparkConf: ")
    val sparkConf = createPipleSparkConf(rootConfig.getConfig("spark"))
    sparkConf.getAll.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })

    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // find all user defined UDFs and register in application init
    UdfRegister.findAndRegisterUdfs(sparkSession)

    StreamingPipelineRunner.preparePipelineRecursively(sparkSession, rootPipeline)

    // when you see this ASCII logo, waterdrop is really started.
    showWaterdropAsciiLogo()

    rootPipelineType match {

      case Streaming => {

        val sparkConfig = rootConfig.getConfig("spark")
        val duration = sparkConfig.getLong("spark.streaming.batchDuration")
        val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(duration))
        StreamingPipelineRunner.pipelineRunnerForStreaming(rootPipeline, sparkSession, ssc)

        ssc.start()
        ssc.awaitTermination()
      }
      case Batch => {
        StreamingPipelineRunner.pipelineRunnerForBatch(rootPipeline, sparkSession)
      }
    }
  }
  private def basePrepare(sparkSession: SparkSession, plugins: List[Plugin]*): Unit = {
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        p.prepare(sparkSession)
      }
    }
  }
  private def registerTempView(staticInputs: List[BaseStaticInput], sparkSession: SparkSession): Unit = {
    var datasetMap = Map[String, Dataset[Row]]()
    for (input <- staticInputs) {

      val ds = input.getDataset(sparkSession)

      val config = input.getConfig()
      config.hasPath("table_name") match {
        case true => {
          val tableName = config.getString("table_name")

          datasetMap.contains(tableName) match {
            case true =>
              throw new ConfigRuntimeException(
                "Detected duplicated Dataset["
                  + tableName + "], it seems that you configured table_name = \"" + tableName + "\" in multiple static inputs")
            case _ => datasetMap += (tableName -> ds)
          }

          ds.createOrReplaceTempView(tableName)
        }
        case false => {
          throw new ConfigRuntimeException(
            "Plugin[" + input.name + "] must be registered as dataset/table, please set \"table_name\" config")
        }
      }
    }
  }
  private def baseCheckConfig(plugins: List[Plugin]*): Unit = {
    var configValid = true
    for (pluginList <- plugins) {
      for (p <- pluginList) {
        val (isValid, msg) = Try(p.checkConfig) match {
          case Success(info) => {
            val (ret, message) = info
            (ret, message)
          }
          case Failure(exception) => (false, exception.getMessage)
        }

        if (!isValid) {
          configValid = false
          printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
        }
      }

      if (!configValid) {
        System.exit(-1) // invalid configuration
      }
    }
    deployModeCheck()
  }
  private def listener(sparkSession: SparkSession): Unit = {
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        //do something


      }
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        //do listener
        if(event.progress.eventTime.containsKey("min")){
          val event_time=event.progress.eventTime.get("min").toString
          for (i <- 0 until event.progress.sources.length){
            println(i + "==========event.progress.sources=======" + event.progress.sources(i).startOffset)
            if(!event.progress.sources(i).startOffset.isEmpty){
              import sparkSession.implicits._

              val schema = new StructType()
                .add("time", StringType,true).add("message", StringType, true)
              var datar=sparkSession.sqlContext.sparkContext.parallelize(event.progress.sources.map(p => {
                Row(event_time,p.startOffset.toString)
              }))
              sparkSession.sqlContext.createDataFrame(datar,schema).write.mode(SaveMode.Overwrite).parquet("/tmp/000000")
              println("*********************************************insert ok*********************************")
              sparkSession.read.parquet("/tmp/000000/*").select().printSchema()

            }
          }
        }
      }
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        //do something


      }
    })
  }
  private def deployModeCheck(): Unit = {
    Common.getDeployMode match {
      case Some(m) => {
        if (m.equals("cluster")) {


          println("preparing cluster mode work dir files...")

          // plugins.tar.gz is added in local app temp dir of driver and executors in cluster mode from --files specified in spark-submit
          val workDir = new File(".")
          //org.apache.spark.internal.Logging.logWarning("work dir exists: " + workDir.exists() + ", is dir: " + workDir.isDirectory)

          workDir.listFiles().foreach(f => println("\t list file: " + f.getAbsolutePath))

          // decompress plugin dir
          val compressedFile = new File("plugins.tar.gz")

          Try(CompressionUtils.unGzip(compressedFile, workDir)) match {
            case Success(tempFile) => {
              Try(CompressionUtils.unTar(tempFile, workDir)) match {
                case Success(_) => println("succeeded to decompress plugins.tar.gz")
                case Failure(ex) => {
                  println("failed to decompress plugins.tar.gz", ex)
                  sys.exit(-1)
                }
              }

            }
            case Failure(ex) => {
              println("failed to decompress plugins.tar.gz", ex)
              sys.exit(-1)
            }
          }
        }
      }
    }
  }
  }
