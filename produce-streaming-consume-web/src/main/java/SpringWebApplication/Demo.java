package SpringWebApplication;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import config.KafkaConfiguration;
import config.KafkaConsumerRunner;
import config.KafkaProducerRunner;
import config.KafkaStreamingLogic;

public class Demo {
	
	public static void runDemo(String intputTopic, String outputTopic) throws InterruptedException{
		
		// Producer Demo
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);	
		props.put("metrics.recording.level", "DEBUG");		
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// sent data to kafkatopic streams-file-input:
		KafkaProducerRunner ProducerRunner = new KafkaProducerRunner(props, intputTopic);
		new Thread(ProducerRunner).start(); //producer will close after finished reading the input

		// Stream Analysis DEMO
		
		//Stream processing is not easy if you choose to 
		
		//(1) DIY: 
		//while(consumerisRunning){
//			message = consumer.poll();

//			DIY your analysis here:

		//  producer.sent(message);
		//}
		//How do you Ordering the messages if you get them from different topics?
		//How do you Partitioning the messages and Scale out your processing?
		//How do you handle fault tolerance&re-processing the data
		//How do you manage the state of your windowing analysis to achieve exactly-one analysis 
		
		//(2) reply on full-fledged stream processing system:
		//Storm, Spark, Samza
		//

		//(3) Streams API 
		//A unique feature of the Kafka Streams API is that the applications you build with it are normal Java applications. 
		//These applications can be packaged, deployed, and monitored like any other Java application – 
		//there is no need to install separate processing clusters or similar special-purpose and expensive infrastructure!
		
		Properties config = new Properties();
		props.put("metrics.recording.level", "DEBUG");
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-analysis");
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once");
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "20000");
		config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "6");//The number of threads to execute stream processing
		config.put(StreamsConfig.POLL_MS_CONFIG, "100");//The amount of time in milliseconds to block waiting for input.
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BrokerURL);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// use 1 stream client with 6 threads to analysis
		KStreamBuilder builder = KafkaStreamingLogic.TputByMarket_LogicBuilder(intputTopic,outputTopic);
		KafkaStreams streams1 = new KafkaStreams(builder, config);
		streams1.cleanUp();
		streams1.start();
		
		// CONSUMER DEMO
		props = new Properties();
		props.put("metrics.recording.level", "DEBUG");
		props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);
		props.put("group.id", "JAVAConsumerGroup1");
		props.put("session.timeout.ms", "100000");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");//The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if enable.auto.commit is set to true
		//props.put("auto.offset.reset", "earliest");
		props.put("auto.offset.reset", "latest");	
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// use three consumers clients to consume
		boolean sentToWebSocket = true;
		//sentToWebSocket = false;
		KafkaConsumerRunner ConsumerRunner1 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		KafkaConsumerRunner ConsumerRunner2 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		KafkaConsumerRunner ConsumerRunner3 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		new Thread(ConsumerRunner1).start();
		new Thread(ConsumerRunner2).start();
		new Thread(ConsumerRunner3).start();
		
		TimeUnit.MILLISECONDS.sleep(1000 * 1000);
		
		ProducerRunner.getProducer().close();
		streams1.close();
		streams1.cleanUp();
		ConsumerRunner1.shutdown();
		ConsumerRunner2.shutdown();
		ConsumerRunner3.shutdown();
	}
	
	
	public static void main(String[] args){
			String a = "Date:20170704_Region:_Market:";
			String[] k = a.split(":");
			System.out.println(k.length);
	}
}
