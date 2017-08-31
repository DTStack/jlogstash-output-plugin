package com.dtstack.jlogstash.format;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author sishu.yss
 *
 */
public abstract class HdfsOutputFormat implements  OutputFormat {

	public static String slash = "/";
    protected static final int NEWLINE = 10;
    protected String charsetName = "UTF-8";
    protected transient Charset charset;
    protected String writeMode;
    protected transient boolean overwrite;
    protected String compress;
    protected String defaultFS;
    protected String path;
    protected String fileName;
    protected String delimiter;

    protected List<String> columnNames;
    protected String[] columnTypes;
    protected String[] inputColumnNames;
    protected String[] inputColumnTypes;

    protected  String outputFilePath;
    protected  FileOutputFormat outputFormat;
    protected  JobConf conf;
    protected  Map<String, String> columnNameTypeMap;
    protected  Map<String, Integer> columnNameIndexMap;
    protected  RecordWriter recordWriter;

    @Override
    public void configure() {
        if (this.writeMode == null || this.writeMode.length() == 0 || this.writeMode.equalsIgnoreCase("APPEND"))
            this.overwrite = true;
        else if (this.writeMode.equalsIgnoreCase("NONCONFLICT"))
            this.overwrite = false;
        else
            throw new IllegalArgumentException("Unsupported WriteMode");

        this.outputFilePath = defaultFS + path + "/" + fileName;

        if(!overwrite) {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.default.name", defaultFS);

            FileSystem fs = null;
            try {
                fs = FileSystem.get(conf);
                if(fs.exists(new Path(outputFilePath)))
                    throw new RuntimeException("nonConflict, you know that.");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }

        }

        conf = new JobConf();

        columnNameTypeMap = new HashMap<>();
        columnNameIndexMap = new HashMap<>();
        for(int i = 0; i < columnNames.size(); ++i) {
            columnNameTypeMap.put(columnNames.get(i), columnTypes[i]);
            columnNameIndexMap.put(columnNames.get(i), i);
        }

    }

    public abstract void writeRecord(Map<String,Object> row) throws IOException;

    @Override
    public void close() throws IOException {
        RecordWriter rw = this.recordWriter;
        if(rw != null) {
            rw.close(Reporter.NULL);
            this.recordWriter = null;
        }
    }


}
