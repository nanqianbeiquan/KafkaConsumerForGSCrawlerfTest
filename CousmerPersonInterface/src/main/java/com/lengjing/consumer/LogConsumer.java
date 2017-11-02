package com.lengjing.consumer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.dom4j.DocumentException;


public class LogConsumer {
	private ConsumerConfig config;
	private String topic;
	private int partitionsNum;
	private MessageExecutor executor;
	private ConsumerConnector connector;
	private ExecutorService threadPool;
	private static FileWriter writer;
	
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
				try {
					//解析xml数据
					try {
						//str=URLDecoder.decode(str,"utf8");
						System.out.println(str);
						String value=str.substring(0,str.indexOf("<?xml"));
						String personName="";
						String companyname="";
						String name=value.split("\\|")[0];
						String idcard="";
						if(name.indexOf("_")>0){
							personName=name.split("_")[0];
							companyname=name.split("_")[1];
						}else{
							personName=name;
							companyname=value.split("\\|")[1];
							idcard=value.split("\\|")[1];
						}
						String valuexml=str.substring(str.indexOf("<?xml"));
						XmlParse.parse(valuexml,personName,companyname,idcard);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						String date=sdf.format(new Date())+".log";
						File f=new File("/home/zyx/hbase/dt");
						File filehive = new File(f,date);
						if(!filehive.exists()){
							filehive.createNewFile();
							FileWriter writer =new FileWriter(filehive,false);
							writer.write(str);
							writer.write("\n");
							writer.close();
						}else{
							//进行追加
							FileWriter writer = new FileWriter(filehive,true);
							writer.write(str);
							writer.write("\n");
							writer.close();
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (DocumentException e) {
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
			consumer = new LogConsumer("zhongshuPersonByPositionTest", 3, executor);
			consumer.start();

			/*Date dNow = new Date(); // 当前时间
			Date dBefore = new Date();
			Calendar calendar = Calendar.getInstance(); // 得到日历
			calendar.setTime(dNow);// 把当前时间赋给日历
			calendar.add(Calendar.DAY_OF_MONTH, -1); // 设置为前一天
			dBefore = calendar.getTime(); // 得到前一天的时间
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String day = sdf.format(dBefore); // 格式化前一天*/
		}catch(Exception e){
			e.printStackTrace();
		}finally{

		}

	}
	
	

}
