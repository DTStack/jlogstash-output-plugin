package com.dtstack.logstash.outputs;

import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.Required;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Kafka extends BaseOutput {
	
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);

	private static ObjectMapper objectMapper = new ObjectMapper();
	
	@SuppressWarnings("rawtypes")
	private Producer producer;
	
	private static String encoding="utf-8";
	
	@Required(required=true)
	private static String topic;
	
	@Required(required=true)
	private static String brokerList;
	
	private static String keySerializer="kafka.serializer.StringEncoder";
	
	private static String valueSerializer="kafka.serializer.StringEncoder";	
	
	private static String partitionerClass = "kafka.producer.DefaultPartitioner";
	
	private static String producerType ="sync";//sync async
	
	private static String compressionCodec = "none";//gzip,snappy,lz4,none
	
	private static String clientId ="";
	
	private static Long batchNum = 0l;
	
	private static Integer requestRequiredAcks=1;

	public Kafka(Map config) {
		super(config);
	}

	public void prepare() {
		try{
			Properties props = new Properties();
			props.put("metadata.broker.list",brokerList);
			props.put("key.serializer.class", keySerializer);
			props.put("value.serializer.class", valueSerializer);
			props.put("partitioner.class", partitionerClass);
			props.put("producer.type", producerType);
			props.put("compression.codec", compressionCodec);
			props.put("request.required.acks", String.valueOf(requestRequiredAcks));
			if(StringUtils.isNotBlank(clientId)){
				props.put("client.id", clientId);			
			}
			if(batchNum>0){
				props.put("batch.num.messages", String.valueOf(batchNum));
			}
			ProducerConfig pconfig = new ProducerConfig(props);
			producer = new Producer<>(pconfig);
		}catch(Exception e){
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@SuppressWarnings("rawtypes")
	protected void emit(Map event) {
		try {
			producer.send(new KeyedMessage<>(topic, objectMapper.writeValueAsString(event).getBytes(encoding)));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
