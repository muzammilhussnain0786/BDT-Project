package cs523.project_spark.consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import cs523.project_spark.hive.SparkHive;
import scala.Tuple2;

public class SparkKafkaConsumer {
	
	private static String kafkaTopic = "logs";
	private static SparkHive sparkHive = new SparkHive();
	
	public static void main(String[] args){
		
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("spark-kafka").setMaster("local[*]"));
		JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(5000));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark-kafka-consumer");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Collections.singleton(kafkaTopic);
		
		JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferBrokers(),
			    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		JavaPairDStream<String, String> pairs = directKafkaStream.mapToPair(record -> new Tuple2<String, String>(record.key(), record.value()));
		
		
		sparkHive.createTable();
		
		
		pairs.foreachRDD(pairRDD -> {
			
			pairRDD.saveAsTextFile("/user/cloudera/hive_input");
			
			List<Tuple2<String, String>> listOfTupples = pairRDD.collect();
			listOfTupples.forEach(tuple -> {
				
				String line = tuple._2;
				System.out.println( line);
				
			});
			
			
		});
		
		jsc.start();
		
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
