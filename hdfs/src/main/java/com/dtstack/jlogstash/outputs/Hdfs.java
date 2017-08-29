package com.dtstack.jlogstash.outputs;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;

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
	private static String path = null ;
	
	private static Configuration configuration = null;
	

	public Hdfs(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		try {
			setConfiguration();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("",e);
			System.exit(-1);
		}
	}

	@Override
	protected void emit(Map event) {
		// TODO Auto-generated method stub
		
	}
	
	
	private void setConfiguration() throws Exception{
		if(configuration == null){
			synchronized(Hdfs.class){
				if(configuration == null){
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

}
