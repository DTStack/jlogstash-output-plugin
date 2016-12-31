/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.logstash.outputs;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
	
	private Map<String,Map<String,Object>> topicSelect;
	
	private Set<Map.Entry<String,Map<String,Object>>> entryTopicSelect;

	@Required(required=true)
	private String brokerList;
	
	private Map<String,String> producerSettings;
	
	@SuppressWarnings("rawtypes")
	public Kafka(Map config) {
		super(config);
	}
	
	/**
	 * default
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
			if(topicSelect!=null){
				entryTopicSelect = topicSelect.entrySet();
			}
			
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
			String tp = null;
			if(entryTopicSelect != null){
				for(Map.Entry<String,Map<String,Object>> entry:entryTopicSelect){
					String key = entry.getKey();
					Map<String,Object> value = entry.getValue();
					List<String> equals = (List<String>) value.get("equal");
					if(equals.contains(event.get(key))){
						tp = Formatter.format(event, (String)value.get("topic"), timezone);
						break;
					}
				}
			}
			if(tp==null){
				tp = Formatter.format(event, topic, timezone);
			}
			producer.send(new KeyedMessage<>(tp, event.toString(), objectMapper.writeValueAsString(event).getBytes(encoding)));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
	 public static void main(String[] args){
		 
	 }
}
