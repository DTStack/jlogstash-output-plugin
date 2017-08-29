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

import com.dtstack.jlogstash.outputs.BaseOutput;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@SuppressWarnings("serial")
public class WebHdfs extends BaseOutput {

	private static final Logger logger = LoggerFactory.getLogger(WebHdfs.class);

	private static String mode = "orc";//orc or text

	private static String hdfsBasePath;//hdfs 根路径

	private static String split = ","; //默认的 text 写入方式的列分隔符

	private static String NAME_NODE_PRESTR = "nn";

	//clusterName 是可以任意指定的，跟集群配置无关
	public static String clusterName = "hdfsClusterName";

	private static final String HADOOP_URL = "hdfs://"+clusterName;

	public static Configuration conf;

	public static String nameNodeAddresses;


	public WebHdfs(Map config) {
		super(config);
	}

	public void initHadoopConf(){

		if(nameNodeAddresses == null){
			throw new RuntimeException("hdfs output plugin must set nameNodeAddresses parameter.");
		}

		String[] nameNodeAddrArray = nameNodeAddresses.split(";");
		int nameNodeSize = nameNodeAddrArray.length;

		String nameNodesStr = "";
		for(int i=1; i<=nameNodeSize; i++){
			String nameNodeAddr = nameNodeAddrArray[i].trim();
			String nameNodeStr = NAME_NODE_PRESTR + i;
			if(!nameNodeAddr.contains(":")){
				throw new RuntimeException("name node address format error! must like ip:port;ip:port...");
			}

			conf.set("dfs.namenode.rpc-address." + clusterName + nameNodeStr, nameNodeAddr);
			nameNodesStr = nameNodesStr + nameNodeStr + ",";
		}

		nameNodesStr = nameNodesStr.substring(0, nameNodesStr.length() - 1);
		conf = new Configuration();
		conf.set("fs.defaultFS", HADOOP_URL);
		conf.set("dfs.nameservices", clusterName);
		conf.set("dfs.ha.namenodes." + clusterName, nameNodesStr);
		conf.set("dfs.client.failover.proxy.provider." + clusterName,
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

		logger.info("-------------init hadoop conf-----------------");
		logger.info(conf.toString());
		logger.info("----------------------------------------------");
	}

	@Override
	public void prepare() {
		initHadoopConf();
	}

	@Override
	protected void emit(Map event) {
		
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("");
		sb.append("HDFS MODE:")
				.append(mode)
				.append("HDFS_BASE_PATH:")
				.append(hdfsBasePath);

		return sb.toString();
	}
}
