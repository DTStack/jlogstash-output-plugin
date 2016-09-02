package com.dtstack.logstash.outputs;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:36:42
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Stdout extends BaseOutput {
	
	private static final Logger logger = LoggerFactory.getLogger(Stdout.class);

	public Stdout(Map config) {
		super(config);
	}

	@Override
	public void prepare() {

	}

	@Override
	protected void emit(Map event) {
		System.out.println(event);
	}
}
