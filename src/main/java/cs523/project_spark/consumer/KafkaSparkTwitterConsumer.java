package cs523.project_spark.consumer;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class KafkaSparkTwitterConsumer {
	
	
	public static void main(String[] args) throws StreamingQueryException,
			TimeoutException {

		Logger.getLogger("org").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder()
				.appName("Spark Kafka Integration Structured Streaming")
				.master("local[*]").getOrCreate();

		Dataset<Row> ds = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "logs")
				.load();

		Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");

		Dataset<Row> dataAsSchema = lines
				.selectExpr("value", "split(value,',')[0] as createdAt",
						"split(value,',')[1] as UsertName",
						"split(value,',')[2] as ScreenName",
						"split(value,',')[3] as FollowersCount",
						"split(value,',')[4] as FriendsCount",
						"split(value,',')[5] as FavouritesCount",
						"split(value,',')[6] as Location",
						"split(value,',')[7] as RetweetCount",
						"split(value,',')[8] as FavoriteCount",
						"split(value,',')[9] as Lang",
						"split(value,',')[10] as Source").drop("value");

		dataAsSchema = dataAsSchema
				.withColumn(
						"createdAt",
						functions.regexp_replace(functions.col("createdAt"),
								" ", ""))
				.withColumn(
						"UsertName",
						functions.regexp_replace(functions.col("UsertName"),
								" ", ""))
				.withColumn(
						"ScreenName",
						functions.regexp_replace(functions.col("ScreenName"),
								" ", ""))
				.withColumn(
						"FollowersCount",
						functions.regexp_replace(functions.col("FollowersCount"), " ", ""))
				.withColumn(
						"FriendsCount",
						functions.regexp_replace(functions.col("FriendsCount"),
								" ", ""))
				.withColumn(
						"FavouritesCount",
						functions.regexp_replace(functions.col("FavouritesCount"), " ", ""))
				.withColumn(
						"Location",
						functions.regexp_replace(functions.col("Location"), " ", ""))
				.withColumn(
						"RetweetCount",
						functions.regexp_replace(functions.col("RetweetCount"), " ", ""))
				.withColumn(
						"FavoriteCount",
						functions.regexp_replace(functions.col("FavoriteCount"), " ", ""))
				.withColumn(
						"Lang",
						functions.regexp_replace(functions.col("Lang"), " ", ""))
				.withColumn(
						"Source",
						functions.regexp_replace(functions.col("Source"), " ",
								""));

		dataAsSchema = dataAsSchema
				.withColumn(
						"createdAt",
						functions.col("createdAt").cast(DataTypes.StringType))
				.withColumn(
						"UsertName",
						functions.col("UsertName").cast(DataTypes.StringType))
				.withColumn(
						"ScreenName",
						functions.col("ScreenName").cast(DataTypes.StringType))
				.withColumn(
						"FollowersCount",
						functions.col("FollowersCount").cast(DataTypes.IntegerType))
				.withColumn(
						"FriendsCount",
						functions.col("FriendsCount").cast(DataTypes.IntegerType))
				.withColumn(
						"FavouritesCount",
						functions.col("FavouritesCount").cast(DataTypes.IntegerType))
				.withColumn(
						"Location",
						functions.col("Location").cast(DataTypes.StringType))
				.withColumn(
						"RetweetCount",
						functions.col("RetweetCount").cast(DataTypes.IntegerType))
				.withColumn(
						"FavoriteCount",
						functions.col("FavoriteCount").cast(DataTypes.IntegerType))
				.withColumn(
						"Lang",
						functions.col("Lang").cast(DataTypes.StringType))
				.withColumn(
						"Source",
						functions.col("Source").cast(DataTypes.StringType));

		StreamingQuery query = dataAsSchema.writeStream().outputMode("append")
				.format("console").start();

		dataAsSchema
				.coalesce(1)
				.writeStream()
				.format("csv")
				// can be "orc", "json", "csv", "parquet" etc.
				.outputMode("append")
				.trigger(Trigger.ProcessingTime(10))

				.option("truncate", false)
				.option("maxRecordsPerFile", 10000)
				.option("path",
						"hdfs://localhost:8020/user/cloudera/twitterImport")
				.option("checkpointLocation",
						"hdfs://localhost:8020/user/cloudera/twitterCheckpoint") // args[1].toString()
				.start().awaitTermination();

		query.awaitTermination();

	}

}
