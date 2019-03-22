#!/bin/bash

# TODO: compress plugins/ dir before start-waterdrop.sh

# copy command line arguments
CMD_ARGUMENTS=$@

PARAMS=""
while (( "$#" )); do
  case "$1" in
    -m|--master)
      MASTER=$2
      shift 2
      ;;

    -e|--deploy-mode)
      DEPLOY_MODE=$2
      shift 2
      ;;

    -c|--config)
      CONFIG_FILE=$2
      shift 2
      ;;

      -i|--variable)
      variable=$2
      java_property_value="-D${variable}"
      variables_substitution="${java_property_value} ${variables_substitution}"
      shift 2
      ;;

    --) # end argument parsing
      shift
      break
      ;;

    # -*|--*=) # unsupported flags
    #  echo "Error: Unsupported flag $1" >&2
    #  exit 1
    #  ;;

    *) # preserve positional arguments
      PARAM="$PARAMS $1"
      shift
      ;;

  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"


BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/lib
PLUGINS_DIR=${APP_DIR}/plugins

DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}

DEFAULT_MASTER=local[2]
MASTER=${MASTER:-$DEFAULT_MASTER}

DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}

# scan jar dependencies for all plugins
source ${UTILS_DIR}/file.sh
source ${UTILS_DIR}/app.sh
jarDependencies=$(listJarDependenciesOfPlugins ${PLUGINS_DIR})
JarDepOpts=""
if [ "$jarDependencies" != "" ]; then
    JarDepOpts="--jars $jarDependencies"
fi

FilesDepOpts=""
if [ "$DEPLOY_MODE" == "cluster" ]; then

    ## add config file
    FilesDepOpts="--files ${CONFIG_FILE}"

    ## add plugin files
    FilesDepOpts="${FilesDepOpts},${APP_DIR}/plugins.tar.gz"

    echo ""

elif [ "$DEPLOY_MODE" == "client" ]; then

    echo ""
fi

assemblyJarName=$(find ${LIB_DIR} -name blueline-*.jar)

source ${CONF_DIR}/waterdrop-env.sh

## get spark conf from config file and specify them in spark-submit
function get_spark_conf {
    spark_conf=$(java -cp ${assemblyJarName} io.cjx.blueline.config.ExposeSparkConf ${CONFIG_FILE})
    if [ "$?" != "0" ]; then
        echo "[ERROR] config file does not exists or cannot be parsed due to invalid format"
        exit -1
    fi
    echo ${spark_conf}
}
string_trim() {
    echo $1 | awk '{$1=$1;print}'
}
sparkconf=$(get_spark_conf)
# Spark Driver Options
variables_substitution=$(string_trim "${variables_substitution}")
driverJavaOpts=""
executorJavaOpts=""
clientModeDriverJavaOpts=""
if [ ! -z "${variables_substitution}" ]; then
  driverJavaOpts="${variables_substitution}"
  executorJavaOpts="${variables_substitution}"
  # in local, client mode, driverJavaOpts can not work, we must use --driver-java-options
  clientModeDriverJavaOpts="${variables_substitution}"
fi

## compress plugins.tar.gz in cluster mode
if [ "${DEPLOY_MODE}" == "cluster" ]; then

  plugins_tar_gz="${APP_DIR}/plugins.tar.gz"

  if [ ! -f "${plugins_tar_gz}" ]; then
    cur_dir=$(pwd)
    cd ${APP_DIR}
    tar zcf plugins.tar.gz plugins
    if [ "$?" != "0" ]; then
      echo "[ERROR] failed to compress plugins.tar.gz in cluster mode"
      exit -2
    fi

    echo "[INFO] successfully compressed plugins.tar.gz in cluster mode"
    cd ${cur_dir}
  fi
fi
echo "[INFO] spark conf: ${sparkconf}"
echo "[INFO] JarDepOpts conf: ${JarDepOpts}"
echo "[INFO] FilesDepOpts conf: ${FilesDepOpts}"
echo "[INFO] assemblyJarName conf: ${assemblyJarName}"
echo "[INFO] CMD_ARGUMENTS conf: ${CMD_ARGUMENTS}"

echo "[INFO] clientModeDriverJavaOpts conf: ${clientModeDriverJavaOpts}"
echo "[INFO] executorJavaOpts conf: ${executorJavaOpts}"
echo "[INFO] driverJavaOpts conf: ${driverJavaOpts}"
exec ${SPARK_HOME}/bin/spark-submit --class io.cjx.blueline.BluelineSource \
    --name $(getAppName ${CONFIG_FILE}) \
    --master ${MASTER} \
    --deploy-mode ${DEPLOY_MODE} \
    --driver-java-options "${clientModeDriverJavaOpts}" \
    --conf spark.executor.extraJavaOptions="${executorJavaOpts}" \
    --conf spark.driver.extraJavaOptions="${driverJavaOpts}" \
    ${sparkconf} \
    ${JarDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} ${CMD_ARGUMENTS}
