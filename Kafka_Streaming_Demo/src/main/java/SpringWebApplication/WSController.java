package MyWebSocket1.WS1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;


@Controller
public class WSController {
	private static SimpMessagingTemplate template;
	@Autowired
	WSController(SimpMessagingTemplate template) {	
		 WSController.template = template;
//		 Properties props = new Properties();
//	     props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);
//	     props.put("group.id", "JAVAConsumerGroup1");
//	     props.put("enable.auto.commit", "true");
//	     props.put("auto.offset.reset", "earliest");
//	     props.put("auto.commit.interval.ms", "1000");
//	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//	     String topic = "WordsWithCountsTopic";
//	     KafkaConsumerRunner ConsumerRunner = new KafkaConsumerRunner(props,topic,true);
//	     new Thread(ConsumerRunner).start();
	}
	public static SimpMessagingTemplate getTemplate(){
		return template;
	}
//    @MessageMapping("/hello")
//    public void greeting() throws Exception {
//    		return;
//    }
//    @SendTo("/topic/random")
//    public double produce(String message) throws InterruptedException {
//        Thread.sleep(1000); // simulated delay        
//        return Math.random()+10;
//    }
//    @SubscribeMapping("/topic/random")
//    public double feeback() throws InterruptedException {
//        Thread.sleep(1000); // simulated delay        
//        return Math.random()+10;
//    }
    
}
