sbt package
/home/changzhihao/spark/spark/bin/spark-submit --class "src.main.scala.squid.SQUIDQuery"\
                                               --master spark://172.16.190.107:7077\
                                               --driver-memory 35g\
                                               --executor-memory 25g\
                                               --num-executors 85\
                                               --executor-cores 5\
                                               /home/changzhihao/sigmod/code/scala/squid/target/scala-2.12/test-project_2.12-1.3.13.jar
