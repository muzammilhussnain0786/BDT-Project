package cs523.project_spark.producer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class FileKafkaProducer {
	
	private static String kafkaBrokerEndpoint = "localhost:9092";
	private static String kafkaTopic = "logs";
	private static String inputFile = "input.csv";
	
	
	private Producer<String, String> getProducerProperties(){
		
		Properties properties = new Properties();
		
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerEndpoint);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "CSVKafkaProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());		
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		
		return new KafkaProducer<String, String>(properties);
		
	}

	public static void main(String[] args){
		
		FileKafkaProducer producer = new FileKafkaProducer();
		
		producer.publishMessages();
		
	}

	private void publishMessages() {
		Producer<String, String> producerProperties = getProducerProperties();
		
		try {
			URI inputFileUri = getClass().getClassLoader().getResource(inputFile).toURI();
			
			Stream<String> linesStream = Files.lines(Paths.get(inputFileUri));
			
			linesStream.forEach(line -> {
				
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, UUID.randomUUID().toString(), line);
				
				producerProperties.send(record, (metaData, Exception) -> {
					
					if(metaData != null){
						System.out.println("Csv Data: -> " + record.key() + " | " + record.value());
					}else{
						System.out.println("Error sending Csv record: -> " + record.value());
					}
				});	
				
			});
			
			linesStream.close();
			
		} catch (URISyntaxException|IOException e) {
			e.printStackTrace();
		} 
		
	}
	
}
