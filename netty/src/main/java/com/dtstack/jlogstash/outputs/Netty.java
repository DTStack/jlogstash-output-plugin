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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;
import com.google.common.collect.Maps;

/**
 * netty 客户端
 * FIXME 完善对ssl支持
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月19日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class Netty extends BaseOutput{
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(Netty.class);
   
	@Required(required=true)
	private static int port;
	
	@Required(required=true)
	private static String host;
	
	private NettyClient client;
	
	private static ObjectMapper objectMapper = new ObjectMapper();
		
	/**输出数据格式:替换的变量${var}*/
	private static String format;
	
	private Map<String, String> replaceStrMap = null;
	
	private static String delimiter = System.getProperty("line.separator");
	
	public Netty(Map config){
		super(config);
	}

	@Override
	public void prepare() {
		client = new NettyClient(host, port);
		client.connect();
		formatStr(format);
	}

	@Override
	protected void emit(Map event) {
		
		try{
			String msg = "";
			if(format != null){
				msg = replaceStr(event);
			}else{
				msg = objectMapper.writeValueAsString(event);
			}
			
			client.write(msg + delimiter);
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	private String replaceStr(Map event){
		String outStr = format;
		for(Entry<String, String> tmpEntry : replaceStrMap.entrySet()){
			String key = tmpEntry.getKey();
			String val = tmpEntry.getValue();
			String newStr = (String) event.get(val);
			if(newStr == null){
				continue;
			}
			
			outStr = outStr.replace(key, newStr);
		}
		
		return outStr;
	}
	
	private void formatStr(String format){
		
		if(this.format == null){
			return;
		}
		
		if(replaceStrMap != null){
			return;
		}
		
		replaceStrMap = Maps.newHashMap();
		Pattern pattern = Pattern.compile("(\\$\\{[a-z0-9A-Z._-]+\\})");
		Matcher matcher = pattern.matcher(format);
		boolean flag = false;
		while(matcher.find()){
			flag = true;
			String replaceStr = matcher.group();
			String str = replaceStr.replace("${", "").replace("}", "");
			replaceStrMap.put(replaceStr, str);
		}
		
		if(!flag){
			logger.error("invalid format str cannot matcher pattern:{}.", "(\\$\\{[a-z0-9A-Z._-]+\\})");
			System.exit(-1);
		}
	}
	
}

class NettyClientHandler extends SimpleChannelHandler {
	
	private static final int CONN_DELAY = 3;
	
	private NettyClient client;
	
	final Timer timer;
	
	public NettyClientHandler(NettyClient client, Timer timer){
		this.client = client;
		this.timer = timer;
	}
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
	
	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	    logger.error("", e);    
    }

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.warn("channel closed.do connect after:{} seconds.", CONN_DELAY);
		//重连
		timer.newTimeout(new TimerTask() {
			
			@Override
			public void run(Timeout timeout) throws Exception {
				ChannelFuture channelfuture = client.getBootstrap().connect();
				client.setChannel(channelfuture);
			}
		}, CONN_DELAY, TimeUnit.SECONDS);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.warn("connect to:{} success.", getRemoteAddress());
	}	
	
	InetSocketAddress getRemoteAddress() {
		return (InetSocketAddress) client.getBootstrap().getOption("remoteAddress");
	}
	
}

class NettyClient{
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
	
	private int port;
	
	private String host;
	
	private volatile Channel channel;
	
	private volatile ClientBootstrap bootstrap;
	
	private final Timer timer = new HashedWheelTimer();
	
	public Object lock = new Object();
				
	public NettyClient(String host, int port){
		this.host = host;
		this.port = port;
	} 
	
	public void connect(){

		bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		bootstrap.setOption("tcpNoDelay", false);
		bootstrap.setOption("keepAlive", true);
		
		final NettyClientHandler handler = new NettyClientHandler(this, timer);
		
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline =  Channels.pipeline();
				pipeline.addLast("handler", handler);
				pipeline.addLast("encoder", new StringEncoder());
				return pipeline;
			}
		});
		
		bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
		try {
			ChannelFuture future = bootstrap.connect().sync();
			channel = future.getChannel();
		} catch (Exception e) {
			logger.error("", e);
			bootstrap.releaseExternalResources();
			System.exit(-1);//第一次连接出现异常直接退出,不走重连
		}
	}
	
	public boolean write(String msg){
		
		boolean canWrite = channel.isConnected() && channel.isWritable();
		while(!canWrite){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("", e);
			}
			canWrite = channel.isConnected() && channel.isWritable();
		}
		
		channel.write(msg);
		return true;
	}

	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	public void setChannel(ChannelFuture channelfuture) {
		this.channel = channelfuture.getChannel();
	}
	
}
