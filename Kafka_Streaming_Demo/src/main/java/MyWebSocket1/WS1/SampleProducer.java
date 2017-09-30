package MyWebSocket1.WS1;

import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.web.client.RestTemplate;

public class SampleProducer implements Runnable{
	private final AtomicBoolean closed = new AtomicBoolean(false);
    private final URI uri;
    private final RestTemplate restTemplate;
    //private final AtomicInteger increment = new AtomicInteger();
    SampleProducer(URI uri, RestTemplate restTemplate){
    	this.uri = uri;  
    	this.restTemplate = restTemplate;
    }
	@Override
	public void run() {
		// TODO Auto-generated method stub
		 while (!closed.get()) { 			 
			String json = "{" +
	"\n\t\"currentSystemTime\": " + 149652995 + "," +
	"\n\t\"currentPlayUnit\": " + 25 + "," +
	"\n\t\"currentBufferedUnit\": " + 30 + "," +
	"\n\t\"incrementalBufferSizeVariation\": " + 4 + "," +
	"\n\t\"startupStall\": [{" +
		"\n\t\t\"startupStallTime\": " + 0.411 + "," +
		"\n\t\t\"parameters\": {" +
			"\n\t\t\t\"startupBegin\": " + 1.2345 + "," +
			"\n\t\t\t\"startupEnd\": " + 1.645 +
		"\n\t\t}" +
	"\n\t}]," +
	"\n\t\"latestBufferingEvent\": [{" +
		"\n\t\t\"bufferingTime\": " + 1.245 + "," +
		"\n\t\t\"parameters\": {" +
		"\n\t\t\t\"bufferingBegin\": " + 10.412 + "," +
		"\n\t\t\t\"bufferingEnd\": " + 11.657 +
		"\n\t\t}" +
	"\n\t}]," +
	"\n\t\"currentPlayStatus\": \"BUFFERING\"," +
	"\n\t\"currentvideoThroughput\": " + (2.5364 + ThreadLocalRandom.current().nextInt(-1, 2)) +
"\n}";
			System.out.println(this.restTemplate.postForObject(this.uri, json, String.class));
			try {
				TimeUnit.MILLISECONDS.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	}
    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
    }
}
