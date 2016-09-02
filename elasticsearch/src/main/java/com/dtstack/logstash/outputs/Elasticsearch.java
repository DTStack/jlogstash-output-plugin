package com.dtstack.logstash.outputs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings.Builder;

import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.render.Formatter;
import com.dtstack.logstash.render.FreeMarkerRender;
import com.dtstack.logstash.render.TemplateRender;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:21
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Elasticsearch extends BaseOutput {
    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);
    
    @Required(required=true)
    private static String index;
    
	private static String indexTimezone = "UTC";

    private static String documentId;
    
    private static String documentType="logs";
    
    private static String cluster;
    
    @Required(required=true)
    private static List<String> hosts;
    
    private static boolean sniff=true;
    
    private static int bulkActions = 20000; 
    
    private static int bulkSize = 15;
    
    private static int  flushInterval = 1;
    
    private static int	concurrentRequests = 1;
    
    private BulkProcessor bulkProcessor;
    
    private TransportClient esclient;
    
    private TemplateRender indexTypeRender =null;
    
    private TemplateRender idRender =null;

    public Elasticsearch(Map config) {
        super(config);
    }

    public void prepare() {
      try {
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


    @SuppressWarnings("unchecked")
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

        bulkProcessor = BulkProcessor
                .builder(esclient, new BulkProcessor.Listener() {

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          BulkResponse arg2) {
                    	
                        ato.getAndSet(1);
//                        logger.info("bulk done with executionId: " + arg0);
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
                                        bulkProcessor.add(requests.get(item
                                                .getItemId()));
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

                        if (totalFailed > 0) {
                            logger.info(totalFailed + " doc failed, "
                                    + toberetry + " need to retry");
                        } else {
                            logger.debug("no failed docs");
                        }

                        if (toberetry > 0) {
                            try {
                                logger.info("sleep " + toberetry / 2
                                        + "millseconds after bulk failure");
                                Thread.sleep(toberetry / 2);
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        } else {
                            logger.debug("no docs need to retry");
                        }

                    }

                    @Override
                    public void afterBulk(long arg0, BulkRequest arg1,
                                          Throwable arg2) {
                    	ato.getAndSet(2);
                        logger.error("bulk got exception");
                        String message = arg2.getMessage();
                        logger.error(message);
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

    protected void emit(Map event) {
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
    }
    
    
    public static void main(String[] args) throws InterruptedException{
        Map<String,Object> event = Maps.newConcurrentMap();
        event.put("tenant_id",4);
        event.put("@timestamp","2016-07-04T01:40:37.54Z");
    	index ="dtlog-%{tenant_id}-%{+YYYY.MM.dd}";
    	hosts = Lists.newArrayList("127.0.0.1:9300");
    	Elasticsearch elasticsearch = new Elasticsearch(new HashMap<String,Object>());
    	elasticsearch.prepare();
    	elasticsearch.emit(event);
    }
}
