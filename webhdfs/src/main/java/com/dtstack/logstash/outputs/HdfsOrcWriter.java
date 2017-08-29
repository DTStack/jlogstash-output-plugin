package com.dtstack.logstash.outputs;

import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 写入 hdfs 格式为 orc
 * Date: 2017/8/28
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class HdfsOrcWriter {

    private static final Logger logger = LoggerFactory.getLogger(HdfsOrcWriter.class);

    private OrcSerde orcSerde;

    private StructObjectInspector inspector;

    private FileOutputFormat outputFormat;

    //FIXME 提供缓存--->用于对对个的tablename做判断
    private JobConf conf;

    public void init(){
        orcSerde = new OrcSerde();
        outputFormat = new OrcOutputFormat();

    }

    public StructObjectInspector createInspector(List<String> columnNames, List<ObjectInspector> columnTypes){
        StructObjectInspector inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypes);
        return inspector;
    }

    public RecordWriter getRecordWriter(String pathStr){

        RecordWriter recordWriter = null;
        try{
            recordWriter = this.outputFormat.getRecordWriter(null, conf, pathStr, Reporter.NULL);
        }catch (Exception e){
            logger.error(e);
        }
        return recordWriter;
    }

    public void write(){
        RecordWriter recordWriter = getRecordWriter();
    }

}
