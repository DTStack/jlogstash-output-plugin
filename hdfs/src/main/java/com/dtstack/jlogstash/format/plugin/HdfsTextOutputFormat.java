package com.dtstack.jlogstash.format.plugin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.format.CompressEnum;
import com.dtstack.jlogstash.format.HdfsOutputFormat;
import com.dtstack.jlogstash.format.util.HostUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class HdfsTextOutputFormat extends HdfsOutputFormat {

	private static Logger logger = LoggerFactory
			.getLogger(HdfsTextOutputFormat.class);

	private String delimiter;

	public HdfsTextOutputFormat(Configuration conf, String outputFilePath,
			List<String> columnNames, List<String> columnTypes,
			String compress, String writeMode, Charset charset, String delimiter) {
		this.conf = conf;
		this.outputFilePath = outputFilePath;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.compress = compress;
		this.writeMode = writeMode;
		this.charset = charset;
		this.delimiter = delimiter;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void configure() {
		super.configure();
		outputFormat = new TextOutputFormat();
		Class<? extends CompressionCodec> codecClass = null;
		if (CompressEnum.NONE.name().equalsIgnoreCase(compress)) {
			codecClass = null;
		} else if (CompressEnum.GZIP.name().equalsIgnoreCase(compress)) {
			codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
		} else if (CompressEnum.BZIP2.name().equalsIgnoreCase(compress)) {
			codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
		} else {
			throw new IllegalArgumentException("Unsupported compress format: "
					+ compress);
		}
		if (codecClass != null) {
			this.outputFormat.setOutputCompressorClass(jobConf, codecClass);
		}
	}

	@Override
	public void open() throws IOException {
        String pathStr = String.format("%s/%s-%d-%s", outputFilePath,HostUtil.getHostName(),Thread.currentThread().getId(),UUID.randomUUID().toString());
		logger.info("hdfs path:{}", pathStr);
		// // 此处好像并没有什么卵用
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String attempt = "attempt_" + dateFormat.format(new Date())
				+ "_0001_m_000000_" + HostUtil.getHostName()+"_"+Thread.currentThread().getId();
		jobConf.set("mapreduce.task.attempt.id", attempt);
		outputFormat.setOutputPath(jobConf, new Path(pathStr));
		this.recordWriter = this.outputFormat.getRecordWriter(null, jobConf,
				pathStr, Reporter.NULL);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeRecord(Map<String, Object> row) throws IOException {
		String[] record = new String[this.columnSize];
		for (int i = 0; i < this.columnSize; i++) {
			Object obj = row.get(this.columnNames.get(i));
			if (obj == null) {
				record[i] = "";
			} else {
				if (obj instanceof Map) {
					record[i] = objectMapper.writeValueAsString(obj);
				} else {
					record[i] = obj.toString();
				}
			}
		}
		recordWriter.write(NullWritable.get(),
				new Text(StringUtils.join(delimiter, record)));
	}

	public class InnerTextOutputFormat<K, V> extends TextOutputFormat {

		@Override
		public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
				JobConf job, String name, Progressable progress)
				throws IOException {
			boolean isCompressed = getCompressOutput(job);
			String keyValueSeparator = job.get(
					"mapreduce.output.textoutputformat.separator", "\t");
			int bufferSize = job.getInt("io.file.buffer.size", 4096);
			if (!isCompressed) {
				Path file = FileOutputFormat.getTaskOutputPath(job, name);
				FileSystem fs = file.getFileSystem(job);
				FSDataOutputStream fileOut = null;
				if(fs.exists(file)){
					fileOut = fs.append(file, bufferSize, progress);
				}else{
					fileOut = fs.create(file,false,bufferSize, progress);
				}
				return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
			} else {
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
						job, GzipCodec.class);
				// create the named codec
				CompressionCodec codec = ReflectionUtils.newInstance(
						codecClass, job);
				// build the filename including the extension
				Path file = FileOutputFormat.getTaskOutputPath(job, name
						+ codec.getDefaultExtension());
				FileSystem fs = file.getFileSystem(job);
				FSDataOutputStream fileOut = null;
				if(fs.exists(file)){
					fileOut = fs.append(file, bufferSize, progress);
				}else{
					fileOut = fs.create(file,false,bufferSize, progress);
				}
				return new LineRecordWriter<K, V>(new DataOutputStream(
						codec.createOutputStream(fileOut)), keyValueSeparator);
			}

		}
	}
}
