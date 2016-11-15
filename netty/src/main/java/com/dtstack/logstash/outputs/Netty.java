package com.dtstack.logstash.outputs;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.outputs.BaseOutput;
import com.google.common.collect.Maps;

/**
 * netty 客户端
 * FIXME 完善对ssl支持
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
		
		while(!client.isConnected()){
			client.tryReconn();
		}
		
		String msg = "";
		if(format != null){
			msg = replaceStr(event);
		}else{
			try {
				msg = objectMapper.writeValueAsString(event);
			} catch (Exception e){
				logger.error("", e);
				return;
			}
		}
		
		client.write(msg + delimiter);
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
	
	public static void main(String[] args) {
		Netty netty = new Netty(new HashMap<String, String>());
		netty.host = "localhost";
		netty.port = 9111;
		netty.format = "${HOSTNAME}${msg}";
		Map<String, String> event = Maps.newHashMap();
		event.put("HOSTNAME", "daxu11");
		event.put("msg", "ddddd");
		
		netty.prepare();
		netty.emit(event);
		System.out.println("stop");
		
		netty.emit(event);
	}
}

class NettyClientHandler extends SimpleChannelHandler {
	
	private NettyClient client;
	
	public NettyClientHandler(NettyClient client){
		this.client = client;
	}
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
	
	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	        logger.error("netty io error: {}",e.getCause());
	        client.chgState(false);
    }	
}

class NettyClient{
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
	
	private int port;
	
	private String host;
	
	private Channel channel;
	
	private ClientBootstrap bootstrap;
	
	public Object lock = new Object();
	
	private AtomicBoolean isConnected = new AtomicBoolean(false);
	
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
		final NettyClientHandler handler = new NettyClientHandler(this);
		
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline =  Channels.pipeline();
				pipeline.addLast("handler", handler);
				pipeline.addLast("encoder", new StringEncoder());
				return pipeline;
			}
		});
		ChannelFuture future;
		try {
			future = bootstrap.connect(new InetSocketAddress(host, port)).sync();
			if(future.isSuccess()){
				logger.warn("----connet to server success----.");
			}
			
			channel = future.getChannel();
			chgState(true);
		} catch (InterruptedException e) {
			logger.error("", e);
			bootstrap.releaseExternalResources();
		}
	}
	
	public void closeConn(){
		bootstrap.releaseExternalResources();
	}
	
	public void chgState(boolean currState){
		isConnected.compareAndSet(!currState, currState);
	}
	
	public boolean write(String msg){
		//如果channel 关闭或者不可用用需要重连
		if(!isConnected.get()){
			return false;
		}
		
		channel.write(msg);
		
		return true;
	}
	
	public boolean isConnected(){
		return isConnected.get();
	}
	
	public void tryReconn(){
		bootstrap.releaseExternalResources();
		logger.warn("---reconnect after 5 second");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			logger.error("", e);
		}
		connect();
	}
	
}
