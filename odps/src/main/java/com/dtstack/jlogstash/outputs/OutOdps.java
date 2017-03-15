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
package com.dtstack.jlogstash.outputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadStatus;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.google.common.collect.Maps;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:59
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class OutOdps extends BaseOutput {
	
	    private static Logger logger = LoggerFactory.getLogger(OutOdps.class);

        @Required(required = true)
        private static String accessId ="LTAIVFhOq0E8Wl54";

        @Required(required = true)
        private static String accessKey = "99TFYSEZtzjFqZ8WRnmEW4rEwC0aDP";

        @Required(required = true)
        private static String odpsUrl = "http://service.odps.aliyun.com/api";

        @Required(required = true)
        private static String project = "dtstack_dev";

        @Required(required = true)
        private static String table = "rec_movies";

        private static String partition;//example dt ='ysq',pt= '123'

        private static UploadSession uploadSession;
        
        private TunnelBufferedWriter recordWriter;
        
        private TableSchema tableSchema ;
        
        private static Long bufferSize;//default 10M
        
        private static TableTunnel tunnel;
        
        private static int interval = 5000;
        
        private Executor executor = Executors.newFixedThreadPool(1);
        
        public OutOdps(Map config) {
                super(config);
                // TODO Auto-generated constructor stub
        }

        @Override
        public void prepare() {
                // TODO Auto-generated method stub
        	try{
        		if(tunnel==null){
        			synchronized(OutOdps.class){
        				if(tunnel==null){
        					tunnel = createTableTunnel();
        				}
        			}
        		}
        		if(uploadSession==null){
        			synchronized(OutOdps.class){
        				if(uploadSession==null){
        					uploadSession = createUploadSession();
        				}
        			}
        		}
                this.tableSchema = uploadSession.getSchema();
                this.recordWriter = createTunnelBufferedWriter();
                executor.execute(new Runnable(){

					@Override
					public void run() {
						// TODO Auto-generated method stub
						try{
							Thread.sleep(interval);
							if(recordWriter!=null)recordWriter.close();
						}catch(Throwable e){
							logger.error("",e);
						}
					}
                });
        	}catch(Throwable e){
        		logger.error("OutOdps init error:",e);
        		System.exit(-1);
        	}
        }
        
        
        
        private TunnelBufferedWriter createTunnelBufferedWriter() throws TunnelException{
        	TunnelBufferedWriter writer =(TunnelBufferedWriter) uploadSession.openBufferedWriter();
        	if(bufferSize!=null){
        		writer.setBufferSize(bufferSize);
        	}
            return writer;
        }
        
        private TableTunnel createTableTunnel(){
        	  Account account = new AliyunAccount(accessId, accessKey);
              Odps odps = new Odps(account);
              odps.setEndpoint(odpsUrl);
              odps.setDefaultProject(project);	
              return new TableTunnel(odps);
        }
        
        public UploadSession createUploadSession() throws TunnelException{
            if(StringUtils.isNotBlank(partition)){
                PartitionSpec partitionSpec = new PartitionSpec(partition);
                return tunnel.createUploadSession(project,table, partitionSpec);
            }else{
            	return tunnel.createUploadSession(project,table);
            }
        }

        @Override
        protected void emit(Map event) {
                // TODO Auto-generated method stub
        	try {
        		while(uploadSession.getStatus().ordinal()!=UploadStatus.NORMAL.ordinal()){
        			Thread.sleep(1000);
        			logger.warn("uploadSession is Expired");
        			uploadSession = createUploadSession();
        			this.recordWriter = createTunnelBufferedWriter();
        		}
				this.recordWriter.write(createRecord(event));
			} catch (Exception e) {
				// TODO Auto-generated catch block
        		logger.error("OutOdps emit error:",e);
			}
        }
        
        @Override
        public void release(){
        	try{
        		this.recordWriter.close();
            	uploadSession.commit();
        	}catch(Throwable e){
        		logger.error("OutOdps release error:",e);
        	}
        }
        
        private Record createRecord(Map event){
        	Record record = uploadSession.newRecord();
            for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                Column column = tableSchema.getColumn(i);
                String name = column.getName();
                Object value = event.get(name);
                if(value!=null){
                    switch (column.getType()) {
                    case BIGINT:
                            record.set(name, Long.parseLong(value.toString()));
                            break;
                    case BOOLEAN:
                           record.set(name, Boolean.parseBoolean(value.toString()));
                            break;
                    case DATETIME:
                    	    record.set(name, value);
                            break;
                    case DOUBLE:
                            record.set(name, Double.parseDouble(value.toString()));
                            break;
                    case STRING:
                            record.set(name, value.toString());
                            break;
                    default:
                            logger.warn("{}:Unknown column type: "+ column.getType(),name);
                    }
                  }
               }
            return record;
         }
        
        public static void main(String args[]) throws TunnelException, IOException {
        	
    		Map<String,Object> event = new HashMap<String,Object>(){
    			{
    				put("title","tttt");
    				put("movieid","tttt");
    				put("genres","tttt");
    			}
    		};
    		OutOdps OutOdps = new OutOdps(Maps.newHashMap());
    		OutOdps.prepare();
    		for(int i=0;i<10;i++){
        		OutOdps.emit(event);
    		}
    		uploadSession.commit();
        }
}
