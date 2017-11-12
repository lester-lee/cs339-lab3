JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
PATH:=${JAVA_HOME}/bin:${PATH}

#find Hadoop path using /home/cs-local-linux/339/hadoop-2.7.4/bin/hadoop classpath
HADOOP_PATH=/home/cs-local-linux/339/hadoop-2.7.4/etc/hadoop:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/common/*:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/hdfs:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/hdfs/lib/*:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/hdfs/*:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/yarn/*:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/mapreduce/lib/*:/home/cs-local-linux/339/hadoop-2.7.4/share/hadoop/mapreduce/*

NEW_CLASSPATH=${HADOOP_PATH}:${CLASSPATH}:lib/*

SRC = $(wildcard *.java)

all: build clean

build: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib

clean:
	rm -rf output temp

run:
	/usr/cs-local/339/hadoop/bin/hadoop jar build.jar ClickRate impressions_merged/ clicks_merged/ output
