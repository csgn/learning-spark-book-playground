SPARK_HOME=/home/${USER}/Playground/learning-spark/spark3
SCALA_VERSION=2.12
PACKAGE_NAME=pg
PACKAGE_VERSION=1.0
TARGET_PACKAGE=${PWD}/target/scala-${SCALA_VERSION}/${PACKAGE_NAME}_${SCALA_VERSION}-${PACKAGE_VERSION}.jar

function run() {
    $SPARK_HOME/bin/spark-submit \
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
