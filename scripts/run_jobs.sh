SPARK_HOME=/home/csgn/Playground/learning-spark/spark3

function mnm() {
    $SPARK_HOME/bin/spark-submit \
        --class chapter2.MnMCount \
        target/scala-2.12/mysources_2.12-1.0.jar \
        data/mnm_dataset.csv
}

# Check the argument passed to the script
if [ $# -eq 0 ]; then
    echo "Usage: $0 <function_name>"
    exit 1
fi

# Determine which function to run based on the argument passed
case "$1" in
    "mnm")
        mnm
        ;;
    *)
        echo "Function '$1' not recognized."
        exit 1
        ;;
esac
