USER = morpheus

EXAMPLE_DIR = /user/$(USER)/spark-wordcount
INPUT_DIR   = $(EXAMPLE_DIR)/input
OUTPUT_DIR  = $(EXAMPLE_DIR)/output
OUTPUT_FILE = $(OUTPUT_DIR)/output.txt
HADOOP_VERSION = 2.8.1 # your hadoop version

TOOLLIBS_DIR=$(HADOOP_HOME)/share/hadoop/tools/lib/
NITERS=100

run: inputs
	spark-submit --master yarn \
	./wordcount-spark.py $(INPUT_DIR)/astro_02 | sort -n -t: -k2 > output.txt
	hdfs dfs -copyFromLocal output.txt $(OUTPUT_DIR)

run-test:
	(unset HADOOP_CONF_DIR; \
	 unset SPARK_YARN_USER_ENV; \
	 $(SPARK_HOME)/spark-submit --master local[2] --deploy-mode client ./wordcount-spark.py ./input/astro_02 | sort -n -t: -k2 )


directories:
			hdfs dfs -test -e $(EXAMPLE_DIR) || hdfs dfs -mkdir $(EXAMPLE_DIR)
			hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir $(INPUT_DIR)
			hdfs dfs -test -e $(OUTPUT_DIR) || hdfs dfs -mkdir $(OUTPUT_DIR)

inputs: directories input/astro_02
			hdfs dfs -test -e $(INPUT_DIR)/astro_02 \
			|| hdfs dfs -copyFromLocal input/astro_02 $(INPUT_DIR)

clean:
			-hdfs dfs -rm -f -r $(INPUT_DIR)
			-hdfs dfs -rm -f -r $(OUTPUT_DIR)

.PHONY: directories inputs clean run run-test
