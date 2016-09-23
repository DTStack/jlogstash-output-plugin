package com.dtstack.logstash.outputs;

import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:36:42
 * Company: www.dtstack.com
 * @author sishu.yss
 * @param <E>
 *
 */
public class Stdout extends BaseOutput {	
	private static final Logger logger = LoggerFactory.getLogger(Stdout.class);
	
	private static String codec="line";
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private static String tempalte="\"%s\" => \"%s\","+System.getProperty("line.separator");

	public Stdout(Map config) {
		super(config);
	}

	@Override
	public void prepare() {

	}

	@Override
	protected void emit(Map event) {
		try{
			if (codec.equals("line")){
				System.out.println(event.get("message"));
			}else if(codec.equals("json_lines")){
				System.out.println(objectMapper.writeValueAsString(event));
			}else if(codec.equals("java_lines")){
				StringBuilder sb = new StringBuilder();
				sb.append("{").append(System.getProperty("line.separator"));
				Set<Map.Entry<String, Object>> entrys=event.entrySet();
				for(Map.Entry<String, Object> entry:entrys){
					sb.append(String.format(tempalte, entry.getKey(),entry.getValue()));
				}
				sb.append("}");
				System.out.println(sb.toString());
			}
		}catch(Exception e){
			logger.error("Stdout emit erro",e.getCause());
		}
	}
}
