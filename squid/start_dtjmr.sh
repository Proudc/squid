sbt package
/home/changzhihao/spark/spark/bin/spark-submit --class "src.main.scala.dtjmr.DTJMR"\
                                               --master spark://172.16.190.107:7077\
                                               --driver-memory 30g\
                                               --executor-memory 20g\
                                               --num-executors 60\
                                               --executor-cores 4\
                                               /home/changzhihao/sigmod/code/scala/squid/target/scala-2.12/test-project_2.12-1.3.13.jar
