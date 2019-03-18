package io.cjx.blueline.config

object CommandLineUtils {

  val parser = new scopt.OptionParser[CommandLineArgs]("start-waterdrop.sh") {
    head("Waterdrop", "1.0.0")

    opt[String]('c', "config").required().action((x, c) => c.copy(configFile = x)).text("config file")
    opt[Unit]('t', "check").action((_, c) => c.copy(testConfig = true)).text("check config")
    opt[String]('e', "deploy-mode")
      .required()
      .action((x, c) => c.copy(deployMode = x))
      .validate(x => if (Common.isModeAllowed(x)) success else failure("deploy-mode: " + x + " is not allowed."))
      .text("spark deploy mode")
    opt[String]('m', "master")
      .required()
      .text("spark master")
    opt[String]('n', "engine")
      .required()
      .action((x, c) => c.copy(engine = x))
      .text("application engine")
  }

}
