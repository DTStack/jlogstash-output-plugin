package com.dtstack.jlogstash.outputs;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.format.HdfsOutputFormat;
import com.dtstack.jlogstash.format.StoreEnum;
import com.dtstack.jlogstash.format.plugin.HdfsOrcOutputFormat;
import com.dtstack.jlogstash.format.plugin.HdfsTextOutputFormat;
import com.dtstack.jlogstash.render.Formatter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author sishu.yss
 *
 */
public class Hdfs extends BaseOutput{
	
	private static final long serialVersionUID = -6012196822223887479L;
	
	private static Logger logger = LoggerFactory.getLogger(Hdfs.class);

	@Required(required = true)
	private static String hadoopConf = System.getenv("HADOOP_CONF_DIR");
	
	@Required(required = true)
	private static String path ;//模板配置
	
	private static String store = "TEXT";
	
	private static String writeMode = "APPEND";
	
	private static String compression = "NONE";
	
	private static String charsetName = "UTF-8";
	
	private static Charset charset;
	
	private static String delimiter = "\001";
	
	public static String timezone;
	
	@Required(required = true)
	private static Map<String,String> schema;
	
	private static List<String> columns;
	
	private static List<String> columnTypes;
	
	private static String hadoopUserName = "root";
	
	private static Configuration configuration;
	
	private Map<String,HdfsOutputFormat> hdfsOutputFormats = Maps.newConcurrentMap();
		
	public Hdfs(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		try {
			formatSchema();
			setHadoopConfiguration();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("",e);
			System.exit(-1);
		}
	}

	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		try{
			String realPath = Formatter.format(event, path, timezone);
			HdfsOutputFormat hdfsOutputFormat = hdfsOutputFormats.get(realPath);
			if(hdfsOutputFormat ==null){
				if(StoreEnum.TEXT.name().equals(store)){
					hdfsOutputFormat = new HdfsTextOutputFormat(configuration,realPath, columns, columnTypes, compression, writeMode, charset, delimiter);
				}else if(StoreEnum.ORC.name().equals(store)){
					hdfsOutputFormat = new HdfsOrcOutputFormat(configuration,realPath, columns, columnTypes, compression, writeMode, charset);
				}
				hdfsOutputFormat.configure();
				hdfsOutputFormat.open();
				hdfsOutputFormats.put(realPath, hdfsOutputFormat);
			}
			hdfsOutputFormat.writeRecord(event);
		}catch(Exception e){
			this.addFailedMsg(event);
			logger.error("",e);
		}
	}
	
	@Override
	public void sendFailedMsg(Object msg){
		emit((Map) msg);
	}
	
	@Override
	public void release(){
		Set<Map.Entry<String, HdfsOutputFormat>> entrys = hdfsOutputFormats.entrySet();
		for(Map.Entry<String, HdfsOutputFormat> entry:entrys){
			try {
				entry.getValue().close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("",e);
			}
		}
	}
	
	private void formatSchema(){
		if(columns == null){
			synchronized(Hdfs.class){
				if(columns == null){
					charset = Charset.forName(charsetName);
					columns = Lists.newArrayList();
					columnTypes = Lists.newArrayList();
					Set<Map.Entry<String,String>> entrys = schema.entrySet();
					for(Map.Entry<String,String> entry:entrys){
						columns.add(entry.getKey());
						columnTypes.add(entry.getValue());
					}
				}
			}
		}
	}
	
	
	private void setHadoopConfiguration() throws Exception{
		if(configuration == null){
			synchronized(Hdfs.class){
				if(configuration == null){
					System.setProperty("HADOOP_USER_NAME", hadoopUserName);
					configuration = new Configuration();
		    		configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		            File[] xmlFileList = new File(hadoopConf).listFiles(new FilenameFilter() {
		                @Override
		                public boolean accept(File dir, String name) {
		                    if(name.endsWith(".xml"))
		                        return true;
		                    return false;
		                }
		            });

		            if(xmlFileList != null) {
		                for(File xmlFile : xmlFileList) {
		                	configuration.addResource(xmlFile.toURI().toURL());
		                }
		            }
				}
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		Hdfs.hadoopConf = "/Users/sishuyss/ysq/dtstack/rdos-web-all/conf/hadoop";
		Hdfs.hadoopUserName = "admin";
		Hdfs.path = "/ysq_test/%{table}";
		Hdfs.store = "ORC";
		Map<String,String> sche = Maps.newConcurrentMap();
		sche.put("table", "varchar");
		sche.put("name","varchar");
		sche.put("year", "int");
		Hdfs.schema = sche;
		Hdfs hdfs = new Hdfs(Maps.newConcurrentMap());
		hdfs.prepare();
		for(int i=0;i<10000;i++){
			Map<String,Object> event = Maps.newConcurrentMap();
			event.put("table", "yxp11");
			event.put("name", "jjj");
			event.put("year", 12);
			hdfs.emit(event);
		}
//		hdfs.release();
	}

}
