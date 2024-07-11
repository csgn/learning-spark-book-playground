SPARK_VERSION=3.5.1
SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop3
SPARK_HOME=${PWD}/${SPARK_NAME}
SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_NAME}.tgz
SCALA_VERSION=2.12
PACKAGE_NAME=pg
PACKAGE_VERSION=1.0
TARGET_PACKAGE=${PWD}/target/scala-${SCALA_VERSION}/${PACKAGE_NAME}_${SCALA_VERSION}-${PACKAGE_VERSION}.jar

function check_or_download_spark() {
    if [ ! -d ${SPARK_HOME} ]; then
        echo "SPARK_HOME is not exist. Downloading..."
        wget ${SPARK_DOWNLOAD_URL}
        if [ -f ${SPARK_NAME}.tgz ]; then
            tar -xvf ${SPARK_NAME}.tgz
            if [ -d ${SPARK_NAME} ]; then
                echo "Setting up Log4j level..."
                cp ${SPARK_NAME}/conf/log4j2.properties.template ${SPARK_NAME}/conf/log4j2.properties
                sed -i 's/rootLogger.level = info/rootLogger.level = error/' "${SPARK_NAME}/conf/log4j2.properties"
                echo "spark-${SPARK_VERSION} downloaded to ${SPARK_HOME} successfully."
            fi
        fi
    else
        echo "Spark already ensured, proceeding..."
    fi
}

function run() {
    check_or_download_spark

    sbt clean package && $SPARK_HOME/bin/spark-submit \
        --deploy-mode client \
        --class ${1}.${2} \
        ${TARGET_PACKAGE} 
}

# Check the argument passed to the script
if [ $# -eq 0 ]; then
    echo "Usage: $0 <package_name> <object_name>"
    exit 1
fi

run $1 $2
