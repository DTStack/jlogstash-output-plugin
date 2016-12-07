package com.dtstack.logstash.outputs;

import java.util.Map;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.render.Formatter;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:11
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Kafka extends BaseOutput {
	
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);

	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private Properties props;
	
	private ProducerConfig pconfig;
	
	private Producer producer;
	
	private String encoding="utf-8";
	
	private String timezone=null;
	
	@Required(required=true)
	private String topic;
	
	@Required(required=true)
	private String brokerList;
	
	private Map<String,String> producerSettings;
	
	@SuppressWarnings("rawtypes")
	public Kafka(Map config) {
		super(config);
	}
	
	/**
	 * default
	 * 
	 * props.put("key.serializer.class", "kafka.serializer.StringEncoder");
	 * props.put("value.serializer.class", "kafka.serializer.StringEncoder");
	 * props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
	 * props.put("producer.type", "sync");
	 * props.put("compression.codec", "none");
	 * props.put("request.required.acks", "1");
	 * props.put("batch.num.messages", "1024");
	 * props.put("client.id", "");			
	 */
	public void prepare() {
		try{
			if(props==null){
				props = new Properties();
			}
			if(producerSettings!=null){
				props.putAll(producerSettings);
			}
			props.put("metadata.broker.list",brokerList);
			if(pconfig==null){
				pconfig = new ProducerConfig(props);
			}
			if(producer==null){
				producer= new Producer(pconfig);
			}
		}catch(Exception e){
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void emit(Map event) {
		try {
			String tp = Formatter.format(event, topic, timezone);
			producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	
	 public static void main(String[] args){
	 }
}
