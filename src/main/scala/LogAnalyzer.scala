import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}


object LogAnalyzer {

  def main(args: Array[String])
  {

            val MAX_COUNT:Int = 50;

            val spark = SparkSession.builder()
              .master("local[1]")
              .appName("SparkByExample")
              .getOrCreate();
           spark.sparkContext.setLogLevel("WARN")
            val ReadLogDF = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("subscribe", "LogPublisher")
              .option("encoding","UTF-8")
              .option("startingOffsets", "earliest")
              .load()
              .selectExpr( "CAST(value AS STRING) as value")


            val ProjectDF =	ReadLogDF.withColumn("IP Address", split(col("value")," - - ")(0)).
              withColumn("temp1",split(col("value"),"\\[")(1)).
              withColumn("Time",split(col("temp1")," ")(0)).
              withColumn("TimeHM",concat(split(col("temp1"),":")(0),lit(" "),split(col("temp1"),":")(1),lit(":"),split(col("temp1"),":")(2))).
              select(col("IP Address"),col("TimeHM"))

            //val FilterDF =ProjectDF.groupBy(col("TimeHM"), col("IP Address")).agg(count(col("IP Address")).as("requestCount")).
            //  filter(col("requestCount") > MAX_COUNT)

             ProjectDF.writeStream.format("console")
            .outputMode("append")
            .start().awaitTermination()
            //spark.stop()
  }



}

