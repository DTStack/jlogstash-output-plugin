package com.dtstack.logstash.outputs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.render.Formatter;
import com.dtstack.logstash.render.FreeMarkerRender;
import com.dtstack.logstash.render.TemplateRender;



/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:21
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Elasticsearch extends BaseOutput {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);
    
    @Required(required=true)
    private static String index;
    
    private static String indexTimezone=null;

    private static String documentId;
    
    private static String documentType="logs";
    
    private static String cluster;
    
    @Required(required=true)
    private static List<String> hosts;
    
    private static boolean sniff=true;
    
    private static int bulkActions = 20000; 
    
    private static int bulkSize = 15;
    
    private static int  flushInterval = 5;//seconds
    
    private static int	concurrentRequests = 1;
        
    private BulkProcessor bulkProcessor;
    
    private TransportClient esclient;
    
    private TemplateRender indexTypeRender =null;
    
    private TemplateRender idRender =null;
    
    private AtomicLong sendReqs = new AtomicLong(0);
    
    private AtomicLong ackReqs = new AtomicLong(0);
   
    private int maxLag = bulkActions;
    
    private AtomicLong needDelayTime = new AtomicLong(0l);
    
    private AtomicBoolean isClusterOn = new AtomicBoolean(true);
    
    private ExecutorService executor;
    
    @SuppressWarnings("rawtypes")
	public Elasticsearch(Map config) {
        super(config);
    }

    public void prepare() {
    	
    	try {
    		executor = Executors.newSingleThreadExecutor();
    		if (StringUtils.isNotBlank(documentId)) {
                idRender = new FreeMarkerRender(documentId,documentId);
            }
             indexTypeRender = new FreeMarkerRender(documentType,documentType);
             this.initESClient();
            } catch (Exception e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
    }


    private void initESClient() throws NumberFormatException,
            UnknownHostException {
        Builder builder  = Settings.settingsBuilder().put("client.transport.sniff", sniff);  
        if(StringUtils.isNotBlank(cluster)){
        	builder.put("cluster.name", cluster);
        }
        Settings settings = builder.build();
        esclient = TransportClient.builder().settings(settings).build();
        for (String host : hosts) {
            String[] hp = host.split(":");
            String h = null, p = null;
            if (hp.length == 2) {
                h = hp[0];
                p = hp[1];
            } else if (hp.length == 1) {
                h = hp[0];
                p = "9300";
            }
            esclient.addTransportAddress(new InetSocketTransportAddress(
                    InetAddress.getByName(h), Integer.parseInt(p)));
        }
        
        executor.submit(new ClusterMonitor(esclient));
        
        bulkProcessor = BulkProcessor
                .builder(esclient, new BulkProcessor.Listener() {

                    @SuppressWarnings("rawtypes")
					@Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          BulkResponse arg2) {
                        List<ActionRequest> requests = arg1.requests();
                        int toberetry = 0;
                        int totalFailed = 0;
                        for (BulkItemResponse item : arg2.getItems()) {
                            if (item.isFailed()) {
                                switch (item.getFailure().getStatus()) {
                                    case TOO_MANY_REQUESTS:
                                    case SERVICE_UNAVAILABLE:
                                        if (toberetry == 0) {
                                            logger.error("bulk has failed item which NEED to retry");
                                            logger.error(item.getFailureMessage());
                                        }
                                        toberetry++;
                                        addFailedMsg(requests.get(item.getItemId()));
                                        break;
                                    default:
                                        if (totalFailed == 0) {
                                            logger.error("bulk has failed item which do NOT need to retry");
                                            logger.error(item.getFailureMessage());
                                        }
                                        break;
                                }

                                totalFailed++;
                            }
                        }
                        
                        addAckSeqs(requests.size());

                        if (totalFailed > 0) {
                            logger.info(totalFailed + " doc failed, "
                                    + toberetry + " need to retry");
                        } else {
                            logger.debug("no failed docs");
                        }

                        if (toberetry > 0) {
                        	  logger.info("sleep " + toberetry / 2
                                      + "millseconds after bulk failure");
                              setDelayTime(toberetry / 2);
                        } else {
                            logger.debug("no docs need to retry");
                        }

                    }

                    @SuppressWarnings("rawtypes")
					@Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          Throwable arg2) {
                        logger.error("bulk got exception:", arg2);
                        
                        for(ActionRequest request : arg1.requests()){
                        	addFailedMsg(request);
                        }
                        
                        addAckSeqs(arg1.requests().size());
                        setDelayTime(1000);
                    }

                    @Override
                    public void beforeBulk(long arg0, BulkRequest arg1) {
                        logger.info("executionId: " + arg0);
                        logger.info("numberOfActions: "
                                + arg1.numberOfActions());
                    }
                })
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(concurrentRequests).build();
    }

    @SuppressWarnings("rawtypes")
	public void emit(Map event) {
        String _index = Formatter.format(event, index, indexTimezone);
        String _indexType = indexTypeRender.render(event);
        IndexRequest indexRequest;
        if (idRender == null) {
            indexRequest = new IndexRequest(_index, _indexType).source(event);
        } else {
            String _id = idRender.render(event);
            indexRequest = new IndexRequest(_index, _indexType, _id)
                    .source(event);
        }
        this.bulkProcessor.add(indexRequest);
        checkNeedWait();
    }
    
    @Override
    public void sendFailedMsg(Object msg){
    	
    	if(needDelayTime.get() >  0){
    		try {
				Thread.sleep(needDelayTime.get());
			} catch (InterruptedException e) {
				logger.error("", e);
			}
    	}
    	
    	this.bulkProcessor.add((IndexRequest)msg);
    	needDelayTime.set(0);
    	checkNeedWait();
    }
    
    public void checkNeedWait(){
    	
    	while(!isClusterOn.get()){//等待集群可用
    		try {
				Thread.sleep(3000);//FIXME
			} catch (InterruptedException e) {
				logger.error("", e);
			}
    	}
    	
    	sendReqs.incrementAndGet();
    	if(sendReqs.get() - ackReqs.get() < maxLag){
    		return;
    	}
    	
    	while(sendReqs.get() - ackReqs.get() > maxLag){
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("", e);
			}
    	}
    }
    
    public void addAckSeqs(int num){
    	ackReqs.addAndGet(num);
    }
    
    public void setDelayTime(long delayTime){
    	if(delayTime > needDelayTime.get()){
    		needDelayTime.set(delayTime);
    	}
    }
    
    class ClusterMonitor implements Runnable{
    	
    	private TransportClient transportClient;
    	
    	public ClusterMonitor(TransportClient client) {
    		this.transportClient = client;
		}

		@Override
		public void run() {
			while(true) {
	    	    try {
	    	        logger.debug("getting es cluster health.");
	    	        ActionFuture<ClusterHealthResponse> healthFuture = transportClient.admin().cluster().health(Requests.clusterHealthRequest());
	    	        ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);
	    	        logger.debug("Get num of node:{}", healthResponse.getNumberOfNodes());
	    	        logger.debug("Get cluster health:{} ", healthResponse.getStatus());
	    	        isClusterOn.set(true);
	    	    } catch(Throwable t) {
	    	        if(t instanceof NoNodeAvailableException){//集群不可用
	    	        	logger.error("the cluster no node avaliable.");
                    	isClusterOn.set(false);
                    }else{
                    	isClusterOn.set(true);
                    }
	    	    }
	    	    try {
	    	        Thread.sleep(3000);//FIXME
	    	    } catch (InterruptedException ie) { 
	    	    	ie.printStackTrace(); 
	    	    }
	    	}
		}	
    }
}
