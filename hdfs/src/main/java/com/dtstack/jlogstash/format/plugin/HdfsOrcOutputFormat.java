package com.dtstack.jlogstash.format.plugin;


import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HdfsOutputFormat;
import com.dtstack.jlogstash.format.HdfsUtil;
import com.dtstack.jlogstash.format.util.ClassUtil;
import com.dtstack.jlogstash.format.util.DateUtil;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 
 * @author sishu.yss
 *
 */
public class HdfsOrcOutputFormat extends HdfsOutputFormat{
	
	private static Logger logger = LoggerFactory.getLogger(HdfsOrcOutputFormat.class);

    private static final long serialVersionUID = 1L;

    private  OrcSerde orcSerde;
    private  StructObjectInspector inspector;
    private  List<ObjectInspector> columnTypeList;


    @Override
    public void configure() {
        this.orcSerde = new OrcSerde();
        this.outputFormat = new OrcOutputFormat();
        this.columnTypeList = new ArrayList<>();
        for(String columnType : columnTypes) {
            this.columnTypeList.add(HdfsUtil.columnTypeToObjectInspetor(columnType));
        }
        this.inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(this.columnNames, this.columnTypeList);

        Class<? extends CompressionCodec> codecClass = null;
        if(compress == null){
            codecClass = null;
        } else if(CompressEnum.GZIP.name().equalsIgnoreCase(compress)){
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        } else if (CompressEnum.BZIP2.name().equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        } else if(CompressEnum.SNAPPY.name().equalsIgnoreCase(compress)) {
            //todo 等需求明确后支持 需要用户安装SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
        } else {
            throw new IllegalArgumentException("Unsupported compress format: " + compress);
        }

        if(codecClass != null)
            this.outputFormat.setOutputCompressorClass(conf, codecClass);
    }

    @Override
    public void open() throws IOException {
            String pathStr = outputFilePath + slash + UUID.randomUUID();
            logger.info("hdfs path:{}",pathStr);
            this.recordWriter = this.outputFormat.getRecordWriter(null, conf, pathStr, Reporter.NULL);
    }

    @Override
    public void writeRecord(Map<String,Object> row) throws IOException {

        Object[] record = new Object[columnNames.size()];
        for(int i = 0; i < row.getArity(); ++i) {
            Object column = row.getField(i);

            String columnName = inputColumnNames[i];
            String fromType = inputColumnTypes[i];
            String toType = columnNameTypeMap.get(columnName);

            if(toType == null) {
                continue;
            }

            if(!fromType.equalsIgnoreCase(toType)) {
                column = ClassUtil.convertType(column, fromType, toType);
            }

            String rowData = column.toString();
            Object field = null;
            switch(toType.toUpperCase()) {
                case "TINYINT":
                    field = Byte.valueOf(rowData);
                    break;
                case "SMALLINT":
                    field = Short.valueOf(rowData);
                    break;
                case "INT":
                    field = Integer.valueOf(rowData);
                    break;
                case "BIGINT":
                    field = Long.valueOf(rowData);
                    break;
                case "FLOAT":
                    field = Float.valueOf(rowData);
                    break;
                case "DOUBLE":
                    field = Double.valueOf(rowData);
                    break;
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                    field = rowData;
                    break;
                case "BOOLEAN":
                    field = Boolean.valueOf(rowData);
                    break;
                case "DATE":
                    field = DateUtil.columnToDate(column);
                    break;
                case "TIMESTAMP":
                    java.sql.Date d = DateUtil.columnToDate(column);
                    field = new java.sql.Timestamp(d.getTime());
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            record[columnNameIndexMap.get(columnName)] = field;

        }

        this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(Arrays.asList(record), this.inspector));
    }


}
