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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.outputs.BaseOutput;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:35:59
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class OutOdps extends BaseOutput {

        @Required(required = true)
        private static String accessId;

        @Required(required = true)
        private static String accessKey;

        @Required(required = true)
        private static String odpsUrl;

        @Required(required = true)
        private static String project;

        @Required(required = true)
        private static String table;

        private static String partition;

        private static int threadNum = 10;

        public OutOdps(Map config) {
                super(config);
                // TODO Auto-generated constructor stub
        }

        @Override
        public void prepare() {
                // TODO Auto-generated method stub
        }

        @Override
        protected void emit(Map event) {
                // TODO Auto-generated method stub

        }

        private static class UploadThread implements Callable<Boolean> {
                private long id;
                private RecordWriter recordWriter;
                private Record record;
                private TableSchema tableSchema;

                public UploadThread(long id, RecordWriter recordWriter, Record record,
                                TableSchema tableSchema) {
                        this.id = id;
                        this.recordWriter = recordWriter;
                        this.record = record;
                        this.tableSchema = tableSchema;
                }

                public Boolean call() throws IOException {
                        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                                Column column = tableSchema.getColumn(i);
                                switch (column.getType()) {
                                case BIGINT:
                                        record.setBigint(i, 1L);
                                        break;
                                case BOOLEAN:
                                        record.setBoolean(i, true);
                                        break;
                                case DATETIME:
                                        record.setDatetime(i, new Date());
                                        break;
                                case DOUBLE:
                                        record.setDouble(i, 0.0);
                                        break;
                                case STRING:
                                        record.setString(i, "sample");
                                        break;
                                default:
                                        throw new RuntimeException("Unknown column type: "
                                                        + column.getType());
                                }
                        }
                        for (int i = 0; i < 10; i++) {
                                try {
                                        recordWriter.write(record);
                                } catch (IOException e) {
                                        recordWriter.close();
                                        e.printStackTrace();
                                        return false;
                                }
                        }
                        recordWriter.close();
                        return true;
                }
        }

        public static void main(String args[]) {
                Account account = new AliyunAccount(accessId, accessKey);
                Odps odps = new Odps(account);
                odps.setEndpoint(odpsUrl);
                odps.setDefaultProject(project);
                try {
                        TableTunnel tunnel = new TableTunnel(odps);
                        PartitionSpec partitionSpec = new PartitionSpec(partition);
                        UploadSession uploadSession = tunnel.createUploadSession(project,
                                        table, partitionSpec);
                        System.out.println("Session Status is : "
                                        + uploadSession.getStatus().toString());
                        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
                        ArrayList<Callable<Boolean>> callers = new ArrayList<Callable<Boolean>>();
                        for (int i = 0; i < threadNum; i++) {
                                RecordWriter recordWriter = uploadSession.openRecordWriter(i);
                                Record record = uploadSession.newRecord();
                                callers.add(new UploadThread(i, recordWriter, record,
                                                uploadSession.getSchema()));
                        }
                        pool.invokeAll(callers);
                        pool.shutdown();
                        Long[] blockList = new Long[threadNum];
                        for (int i = 0; i < threadNum; i++)
                                blockList[i] = Long.valueOf(i);
                        uploadSession.commit(blockList);
                        System.out.println("upload success!");
                } catch (TunnelException e) {
                        e.printStackTrace();
                } catch (IOException e) {
                        e.printStackTrace();
                } catch (InterruptedException e) {
                        e.printStackTrace();
                }
        }

}
