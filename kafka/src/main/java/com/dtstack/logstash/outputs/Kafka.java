package com.dtstack.logstash.outputs;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

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
	
	private Producer producer;
	
	private static String encoding="utf-8";
	
	@Required(required=true)
	private static String topic;
	
	@Required(required=true)
	private static String brokerList;

	public Kafka(Map config) {
		super(config);
	}

	public void prepare() {
		try{
			Properties props = new Properties();
			props.put("metadata.broker.list",brokerList);
			props.put("key.serializer.class", "kafka.serializer.StringEncoder");
//			props.put("request.required.acks", "0");
			ProducerConfig pconfig = new ProducerConfig(props);
			producer = new Producer<>(pconfig);
		}catch(Exception e){
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	protected void emit(Map event) {
		try {
			producer.send(new KeyedMessage<>(topic, objectMapper.writeValueAsString(event).getBytes(encoding)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
