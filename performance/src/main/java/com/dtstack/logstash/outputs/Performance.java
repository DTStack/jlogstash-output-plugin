package com.dtstack.logstash.outputs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
	
	private static int maxSaveday = 8;//0表示无限制
	
	private String timeFormat = "";
	
	private String fileNamePattern = "";
			 	
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
					File newFile = new File(newPath);
					if(!newFile.exists()){
						newFile.createNewFile();
						deleExpiredFile(newFile.getParentFile());
					}
					
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
		compileTimeInfo();
		executor.submit(new PerformanceEventRunnable());
	}
	
	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		eventNumber.getAndIncrement();
	}
	
	public void compileTimeInfo(){
		Pattern filePattern = Pattern.compile("^(.*)(/|\\\\)([^/\\\\]*(\\%\\{\\+?(.*?)\\})\\S+)$");
		Matcher matcher = filePattern.matcher(path);
		if(!matcher.find()){
			logger.error("setting performance path can not matcher to the pattern.");
			return;
		}
		
		String fileName = matcher.group(3);
		String timeStr = matcher.group(4);
		timeFormat = matcher.group(5);
		
		fileNamePattern = fileName.replace(timeStr, "(\\S+)");
	}
	
	public void deleExpiredFile(File dic){
		if(!dic.isDirectory()){
			logger.error("invalid file dictory:{}.", dic.getPath());
			return;
		}
		
		DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat).
				withZone(DateTimeZone.forID(timeZone));
		
		DateTime expiredTime = new DateTime();
		expiredTime = expiredTime.plusDays(0-maxSaveday);
		expiredTime = formatter.parseDateTime(expiredTime.toString(formatter));
		
		Pattern pattern = Pattern.compile(fileNamePattern);
		for(String fileName : dic.list()){
			Matcher matcher = pattern.matcher(fileName);
			if(matcher.find()){
				String timeStr = matcher.group(1);
				DateTime fileDateTime = formatter.parseDateTime(timeStr);
				
				if(fileDateTime.isBefore(expiredTime.getMillis())){
					File deleFile = new File(dic, fileName);
					if(deleFile.exists()){
						logger.info("delete expired file:{}.", fileName);
						deleFile.delete();
					}
				}
			}
		}
	}
}
