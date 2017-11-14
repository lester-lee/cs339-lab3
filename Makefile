JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
PATH:=${JAVA_HOME}/bin:${PATH}

#find Hadoop path using /home/ubuntu/hadoop/bin/hadoop classpath
HADOOP_PATH=/home/ubuntu/hadoop/etc/hadoop:/home/ubuntu/hadoop/share/hadoop/common/*:/home/ubuntu/hadoop/share/hadoop/hdfs:/home/ubuntu/hadoop/share/hadoop/hdfs/lib/*:/home/ubuntu/hadoop/share/hadoop/hdfs/*:/home/ubuntu/hadoop/share/hadoop/yarn/*:/home/ubuntu/hadoop/share/hadoop/mapreduce/lib/*:/home/ubuntu/hadoop/share/hadoop/mapreduce/*

NEW_CLASSPATH=${HADOOP_PATH}:${CLASSPATH}:lib/*

SRC = $(wildcard *.java)

all: build clean

build: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib

clean:
	rm -rf output temp
	hdfs dfs -rm -f -r temp

run:
	/home/ubuntu/hadoop/bin/hadoop jar build.jar ClickRate impressions_merged/ clicks_merged/ output
