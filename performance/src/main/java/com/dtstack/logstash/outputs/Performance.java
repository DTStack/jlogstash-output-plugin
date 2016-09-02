package com.dtstack.logstash.outputs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.date.UnixMSParser;
import com.dtstack.logstash.render.Formatter;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:36:10
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Performance extends BaseOutput{
	
	private static Logger logger = LoggerFactory.getLogger(Performance.class);
	
	private static AtomicLong eventNumber = new AtomicLong(0);
		
	private static int interval = 30;//seconds
	
	private static String timeZone = "UTC";

	@Required(required=true)
	private static String path;
		 	
	private static ExecutorService executor = Executors.newSingleThreadExecutor();
	
	public Performance(Map<String,Object> config){
		super(config);
	}

	class PerformanceEventRunnable implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				BufferedWriter bufferedWriter = null;
				FileWriter fw = null;
				try {
					Thread.sleep(interval*1000);
					StringBuilder sb = new StringBuilder();
					long number =eventNumber.getAndSet(0);
					DateTime dateTime =new UnixMSParser().parse(String.valueOf(Calendar.getInstance().getTimeInMillis()));
					sb.append(dateTime.toString()).append(" ").append(number).append(System.getProperty("line.separator"));
					String newPath = Formatter.format(new HashMap<String,Object>(),path,timeZone);
					fw = new FileWriter(newPath,true);
					bufferedWriter = new BufferedWriter(fw);
					bufferedWriter.write(sb.toString());
					bufferedWriter.flush();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}finally{
					try{
						if (bufferedWriter != null)bufferedWriter.close();
						if (fw!=null)fw.close();
					}catch(Exception e){
						logger.error(e.getMessage());
					}
				}
		
			}
		
		}
		
	}


	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		executor.submit(new PerformanceEventRunnable());
	}
	
	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		eventNumber.getAndIncrement();
	}
	
	public static void main(String[] args){
		interval = 1;
		path = "/Users/sishuyss/ysq.log";
		for(int i=0;i<3;i++){	
			Performance performance = new Performance(new HashMap<String,Object>());
			performance.prepare();	
			performance.emit(new HashMap<String,Object>());
		}

	}
}
