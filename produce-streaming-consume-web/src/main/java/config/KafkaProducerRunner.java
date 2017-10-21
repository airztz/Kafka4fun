package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerRunner implements Runnable {
	private final Properties props;
	private final Producer<String, String> producer;
	private final String topic;
	private final String source;
	private final AtomicBoolean isRunning;
	private final Logger log = LoggerFactory.getLogger(KafkaProducerRunner.class);
	public KafkaProducerRunner(Properties props, String Topic, String Source) {
		this.props = props;
		this.producer = new KafkaProducer<String, String>(props);
		this.topic = Topic;
		this.source = Source;
		this.isRunning = new AtomicBoolean(false);
	}

	public Producer<String, String> getProducer() {
		return this.producer;
	}

	public boolean getStatus() {
		return this.isRunning.get();
	}

	// kafka_2.11-0.10.2.0/bin/kafka-console-consumer.sh --bootstrap-server
	// ip-172-31-0-197:9092,ip-172-31-0-197:9093,ip-172-31-7-234:9094 --topic
	// streams-file-input --from-beginning --formatter
	// kafka.tools.DefaultMessageFormatter --property print.key=true --property
	// print.value=true --property
	// key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
	// --property
	// value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
	@Override
	public void run() {
		log.info("Started reading: {}", this.source);
		this.isRunning.set(true);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(this.source));
			String line = null;
			int i = -1;
			while ((line = reader.readLine()) != null) {
				int comma = 0, third = 0;
				if (++i > 0) // skip header
					while (third++ < line.length()) {
						if (line.charAt(third) == ',')
							comma++;
						if (comma == 3) {
							String date = line.substring(third - 8, third);
							this.producer.send(new ProducerRecord<String, String>(this.topic, date, line));
							break;
						}
					}
				TimeUnit.MILLISECONDS.sleep(100);
			}
			this.isRunning.set(false);
			this.producer.close();
			reader.close();
			log.info("Finished reading: {}", this.source);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			this.isRunning.set(false);
			e.printStackTrace();
		}
	}
}
