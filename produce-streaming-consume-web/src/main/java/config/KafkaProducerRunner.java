package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerRunner implements Runnable{
	private final Properties props;
	private final Producer<String, String> producer;
	private final String topic;
	private boolean isRunning;
	public KafkaProducerRunner(Properties props, String Topic){
   	 this.props = props;
	 this.producer = new KafkaProducer<String,String>(props);
	 this.topic = Topic;	
	 this.isRunning = true;
	}
	public Producer<String, String> getProducer(){
		return this.producer;
	}
	public boolean getStatus(){
		return this.isRunning;
	}
	//kafka_2.11-0.10.2.0/bin/kafka-console-consumer.sh --bootstrap-server ip-172-31-0-197:9092,ip-172-31-0-197:9093,ip-172-31-7-234:9094 --topic streams-file-input --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
	@Override
	public void run(){
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader("alu4g_cell_combined_20170708.csv"));
			String line = null;
			int i =0;
			while ((line = reader.readLine()) != null) {	
				int comma = 0,third = 0;
				if(i>0) //skip header
				while(third++<line.length()){
					if(line.charAt(third)==',') comma++;
					if (comma==3) {
						String date = line.substring(third-8, third);
						this.producer.send(new ProducerRecord<String, String>(this.topic, date,line));
						break;
					}
				}
			    if(i++==100000) break;
			    TimeUnit.MILLISECONDS.sleep(100);
			}
			this.isRunning = false;
			this.producer.close();
			reader.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			this.isRunning = false;
			e.printStackTrace();
		} 		
	}	
}
