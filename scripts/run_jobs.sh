SPARK_HOME=/home/csgn/Playground/learning-spark/spark3
SCALA_VERSION=2.12
PACKAGE_NAME=pg
PACKAGE_VERSION=1.0
TARGET_PACKAGE_JAR=${PWD}/target/scala-2.12/pg

function str() {
    $SPARK_HOME/bin/spark-submit \
        --deploy-mode client \
        --class ch3.Struct \
        target/scala-${SCALA_VERSION}/${PACKAGE_NAME}_${SCALA_VERSION}-${PACKAGE_VERSION}.jar
}

# Check the argument passed to the script
if [ $# -eq 0 ]; then
    echo "Usage: $0 <function_name>"
    exit 1
fi

# Determine which function to run based on the argument passed
case "$1" in
    "str")
        str
        ;;
    *)
        echo "Function '$1' not recognized."
        exit 1
        ;;
esac
