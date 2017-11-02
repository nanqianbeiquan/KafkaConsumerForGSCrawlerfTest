package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


public class LogConsumer {
	private ConsumerConfig config;
	private String topic;
	private int partitionsNum;
	private MessageExecutor executor;
	private ConsumerConnector connector;
	private ExecutorService threadPool;

	
	public LogConsumer(String topic,int partitionsNum,MessageExecutor executor) throws Exception{
		Properties properties = new Properties();
		properties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
		properties.put("auto.offset.reset", "smallest");
		config = new ConsumerConfig(properties);
		this.topic = topic;
		this.partitionsNum = partitionsNum;
		this.executor = executor;
	}
	
	public void start() throws Exception{
		connector = Consumer.createJavaConsumerConnector(config);
		Map<String,Integer> topics = new HashMap<String,Integer>();
		topics.put(topic, partitionsNum);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
		List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
		threadPool = Executors.newFixedThreadPool(partitionsNum);
		for(KafkaStream<byte[], byte[]> partition : partitions){
			threadPool.execute(new MessageRunner(partition));
		} 
	}

    	
	public void close(){
		try{
			threadPool.shutdownNow();
		}catch(Exception e){
			//
		}finally{
			connector.shutdown();
		}
		
	}
	
	class MessageRunner implements Runnable{
		private KafkaStream<byte[], byte[]> partition;
		
		MessageRunner(KafkaStream<byte[], byte[]> partition) {
			this.partition = partition;
		}
		
		public void run(){
			ConsumerIterator<byte[], byte[]> it = partition.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[],byte[]> item = it.next();
				String str=new String(item.message());
				LoadHbaseData data=new LoadHbaseData();
				
				try {	
					data.loadData(str.replace("\r", ""));
					Calendar c=Calendar.getInstance();
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String date=sdf.format(new Date()); 
					File dayfile=new File("/home/zyx/hbaseBatch/dt/"+date); 
					//File dayfile=new File("d:\\yubing.lei\\Desktop\\hivetest\\dt\\"+date);
					if(!dayfile.exists()){
						dayfile.mkdirs();	
					}
					File hour=new File(dayfile,c.get(Calendar.HOUR_OF_DAY)+"");
					if(!hour.exists()){
						hour.createNewFile();
						FileWriter writer =new FileWriter(hour,false);
						writer.write(str);
						writer.write("\n");
						writer.close();
					}else{
						FileWriter writer = new FileWriter(hour,true);
						writer.write(str);
						writer.write("\n");
						writer.close();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
	}
	
	interface MessageExecutor {
		
		public void execute(String message);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LogConsumer consumer = null;

		try{		
			MessageExecutor executor = new MessageExecutor() {			
				public void execute(String message) {
					System.out.println("begin :"+message);																
				}
			};
			consumer = new LogConsumer("GSRealTime", 3, executor);
			consumer.start();
		}catch(Exception e){
			e.printStackTrace();
		}finally{

		}

	}
	
	

}
