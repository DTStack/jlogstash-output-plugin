package com.dtstack.jlogstash.format.plugin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HdfsOutputFormat;
import com.dtstack.jlogstash.format.util.ClassUtil;
import com.dtstack.jlogstash.format.util.DateUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author sishu.yss
 *
 */
public class HdfsTextOutputFormat extends HdfsOutputFormat {

	private static Logger logger = LoggerFactory.getLogger(HdfsTextOutputFormat.class);
	
    public final SimpleDateFormat FIELD_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void configure() {
    	super.configure();
        outputFormat = new TextOutputFormat();
        Class<? extends CompressionCodec> codecClass = null;
        if(compress == null){
            codecClass = null;
        } else if(CompressEnum.GZIP.name().equalsIgnoreCase(compress)){
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        } else if (CompressEnum.BZIP2.name().equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
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
            outputFormat.setOutputPath(conf, new Path(pathStr));
//            // 此处好像并没有什么卵用
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
//            String attempt = "attempt_"+dateFormat.format(new Date())+"_0001_m_000000_" + taskNumber;
//            conf.set("mapreduce.task.attempt.id", attempt);
            this.recordWriter = this.outputFormat.getRecordWriter(null, conf, pathStr, Reporter.NULL);
    }

    @Override
    public void writeRecord(Map<String,Object> row) throws IOException {

        String[] record = new String[columnNames.size()];

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
                    ;                   break;
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
                    rowData = FIELD_DATE_FORMAT.format((Date)field);
                    break;
                case "TIMESTAMP":
                    java.sql.Date d = DateUtil.columnToDate(column);
                    field = new java.sql.Timestamp(d.getTime());
                    rowData = FIELD_DATE_FORMAT.format((Date)field);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            record[columnNameIndexMap.get(columnName)] = rowData;

        }

        for(int i = 0; i < record.length; ++i) {
            if(record[i] == null) {
                record[i] = "";
            }
        }
        recordWriter.write(NullWritable.get(), new Text(StringUtils.join(delimiter, record)));
    }
}
