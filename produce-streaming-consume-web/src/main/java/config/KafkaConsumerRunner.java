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
	private final int pull_waitingTime;
	private final JsonParser Jparser;
	private final AtomicBoolean sentToWebSocket;
	private final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);
	public KafkaConsumerRunner(Properties props, String topic, boolean sentToWebSocket) {
		this.props = props;
		this.pull_waitingTime = (int) props.get("auto.commit.interval.ms");
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
			Map<String, TreeMap<Long,String>> recordKey = new HashMap<String, TreeMap<Long,String>>();
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(pull_waitingTime);
				if (records.count() > 0) {
					//recordKey.clear();
					//log.info("numbers of record per second = {}",records.count());
					for (ConsumerRecord<String, String> record : records) {	
						//if(!recordKey.add(record.key())) continue;
						//log.info("key = {}, value = {}", record.key(), record.value());
						// System.out.printf("key = %s, value = %s\n", record.key(), record.value());
						// System.out.printf("offset = %d, value = %s\n",
						// record.offset(), record.value());
						if (this.sentToWebSocket.get()) {
							//String[] keys = record.key().split(":");
							//if (keys.length < 2)
							//	continue;
							//String market = keys[2];
							//String[] Records = record.value().split(":");
							//Map<String, Object> JsonMap = Jparser.parseMap(record.value())							
							//long start = Long.valueOf(keys[0]);
							
							String market = record.key();				
							if(!recordKey.containsKey(market)) recordKey.put(market, new TreeMap<Long,String>());
							TreeMap<Long,String> windows = recordKey.get(market);
							long window_start = Long.valueOf(record.value().substring(15, 28));
							windows.put(window_start,record.value());	
							if(windows.size()>3) {
								SimpMessagingTemplate template = WSController.getTemplate();
								long start = windows.firstKey();
								Map<String, Object> JsonMap = Jparser.parseMap(windows.get(start));
								String currentTput = (String) JsonMap.get("UTput");
								String count = (String) JsonMap.get("count");
								template.convertAndSend("/topic/currentTput", market + "," + currentTput + "," +  start);
								windows.remove(start);
								log.info("start = {}, market = {}, count = {}, value = {}", start, market, count, currentTput);
							}
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
