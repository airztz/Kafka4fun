package config;

import java.util.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import SpringWebApplication.WSController;

public class KafkaConsumerRunner implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private final Properties props;
	private final JsonParser Jparser;
	private final AtomicBoolean sentToWebSocket;
	private final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);
	public KafkaConsumerRunner(Properties props, String topic, boolean sentToWebSocket) {
		this.props = props;
		this.Jparser = JsonParserFactory.getJsonParser();
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
		this.sentToWebSocket = new AtomicBoolean(sentToWebSocket);
	}

	@Override
	public void run() {
		try {
			// consumer.subscribe(Arrays.asList("test"));
			// TopicPartition partition = new TopicPartition("test", 0);
			// consumer.assign(Arrays.asList(partition));
			// consumer.seek(partition, 0);
			Set<String> recordKey = new HashSet<String>();
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				if (records.count() > 0) {
					recordKey.clear();
					log.info("numbers of record per second = {}",records.count());
					for (ConsumerRecord<String, String> record : records) {	
						if(!recordKey.add(record.key())) continue;
						log.info("key = {}, value = {}", record.key(), record.value());
						// System.out.printf("key = %s, value = %s\n", record.key(), record.value());
						// System.out.printf("offset = %d, value = %s\n",
						// record.offset(), record.value());
						// Map<String, Object> JsonMap =
						// Jparser.parseMap(record.value());
						// Double currentTput = (Double)
						// JsonMap.get("currentvideoThroughput");
						if (this.sentToWebSocket.get()) {
							String[] keys = record.key().split(":");
							if (keys.length < 4)
								continue;
							String market = keys[3];
							String[] Records = record.value().split(":");
							// String currentTput = Records[2];
							String currentTput = Records[2].substring(1, Records[2].length() - 2);
							SimpMessagingTemplate template = WSController.getTemplate();
							template.convertAndSend("/topic/currentTput", market + "," + currentTput);
						}

					}
					if (this.props.getProperty("enable.auto.commit").equals("false"))
						consumer.commitSync();
				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
