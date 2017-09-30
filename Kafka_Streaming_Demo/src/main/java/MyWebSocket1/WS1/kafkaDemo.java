package MyWebSocket1.WS1;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class kafkaDemo {
	
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
		// while(ProducerRunner.getStatus()){
		// continue;
		// }

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
		
		final FractionSerializer FractionSerializer = new FractionSerializer();
		final FractionDeserializer FractionDeserializer = new FractionDeserializer();
		final Serde<Fraction> FractionSerde = Serdes.serdeFrom(FractionSerializer, FractionDeserializer);
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
		KStreamBuilder builder = new KStreamBuilder();
		KStream<String, String> inputRecord = builder.stream(intputTopic);//read input stream
		KTable<String, String> marketUserTput = inputRecord
				.filter((recordKey,
						recordValue) -> (recordValue.length() > 0 && !recordValue.equals("") && recordValue != null))
				.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {//set key to Date:" + recordKey + "_Region:" + region + "_Market:" + market, and set record value to EUCELL_DL_TPUT_NUM_KBITS	EUCELL_DL_TPUT_DEN_SECS	EUCELL_DL_DRB_TPUT_NUM_KBITS	EUCELL_DL_DRB_TPUT_DEN_SECS  
					@Override
					public KeyValue<String, String> apply(String recordKey, String recordValue) {
						int comma = 0, walker = 0, runner = -1;
						String region = null, market = null;
						while (++runner < recordValue.length()) {
							if (recordValue.charAt(runner) != ',')
								continue;
							comma++;
							if (comma == 5)
								walker = runner + 1;
							else if (comma == 6) {
								region = recordValue.substring(walker, runner);
								walker = runner + 1;
							} else if (comma == 7)
								market = recordValue.substring(walker, runner);
							else if (comma == 12)
								break;
						}
						String newKey = "Date:" + recordKey + "_Region:" + region + "_Market:" + market;
						String newRecord = recordValue.substring(runner + 1, recordValue.length());
						return new KeyValue<String, String>(newKey, newRecord);
					}
				}).groupByKey()
				// .groupBy(new KeyValueMapper<String, String, String>() {//new
				// KeyValueMapper<OldKeyType, OldValueType, NewKeyType>
				// @Override
				// //return NewKey
				// public String apply(String recordKey, String recordValue) {
				// String[] record = recordValue.split(",");
				// String region = record[5];
				// String market = record[6];
				// String newKey =
				// "Date:"+recordKey+"_Region:"+region+"_Market:"+market;
				// return newKey;
				// }
				// })
				.aggregate(new Initializer<Fraction>() {
					@Override
					public Fraction apply() {
						return new Fraction(0, 0.0, 0.0);
					}
				}, new Aggregator<String, String, Fraction>() {
					@Override
					public Fraction apply(String recordKey, String recordValue, Fraction aggregate) {
						String[] record = recordValue.split(",");
						Double EUCELL_DL_TPUT_NUM_KBITS = 0.0;
						Double EUCELL_DL_TPUT_DEN_SECS  = 0.0;
						if(record.length!=0){
							EUCELL_DL_TPUT_NUM_KBITS = Double.parseDouble(record[0]);
							EUCELL_DL_TPUT_DEN_SECS = Double.parseDouble(record[1]);	
						}
						aggregate.count++;
						aggregate.numerator += EUCELL_DL_TPUT_NUM_KBITS;
						aggregate.denominator += EUCELL_DL_TPUT_DEN_SECS;
						return aggregate;
					}
				}, FractionSerde)
				.mapValues(new ValueMapper<Fraction, String>() {
					@Override
					public String apply(Fraction aggregate) {
						return "{\"count\":\"" + aggregate.count + "\", \"UTput\":\"" + (aggregate.numerator / aggregate.denominator) + "\"}";
						//return "count:" + aggregate.count + " UTput:" + (aggregate.numerator / aggregate.denominator);
					}
				});
		marketUserTput.to(Serdes.String(), Serdes.String(), outputTopic);
		// use three stream client to analysis
		KafkaStreams streams1 = new KafkaStreams(builder, config);
		streams1.cleanUp();
		streams1.start();
		//KafkaStreams streams2 = new KafkaStreams(builder, config);
		//streams2.start();
		//TimeUnit.MILLISECONDS.sleep(10 * 1000);
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
		// use two consumer clients to consume
		boolean sentToWebSocket = false;
		sentToWebSocket = true;
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
//		streams2.close();
//		streams2.cleanUp();
		ConsumerRunner1.shutdown();
		ConsumerRunner2.shutdown();
		ConsumerRunner3.shutdown();
		//System.exit(0);
	}
	
	
	public static void main(String[] args){
			String a = "Date:20170704_Region:_Market:";
			String[] k = a.split(":");
			System.out.println(k.length);
	}
}
