/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.HiveIcebergFilterFactory;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IcebergMapReduceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMapReduceUtils.class);

  private IcebergMapReduceUtils() {
  }

  public static JobContext newJobContext(JobConf job) {
    JobID jobID = Optional.ofNullable(JobID.forName(job.get(JobContext.ID)))
        .orElseGet(JobID::new);

    return new JobContextImpl(job, jobID);
  }

  public static Table deserializeTable(Configuration conf) {
    String serializedTable = conf.get(InputFormatConfig.SERIALIZED_TABLE, null);
    return (serializedTable != null) ?
        SerializationUtil.deserializeFromBase64(serializedTable) :
        Catalogs.loadTable(conf);
  }

  public static JobConf configureScan(JobConf originalJob) {
    JobConf job = new JobConf(originalJob);
    // Convert Hive filter to Iceberg filter
    String hiveFilter = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (hiveFilter != null) {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities
          .deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      try {
        Expression filter = HiveIcebergFilterFactory.generateFilterExpression(sarg);
        job.set(InputFormatConfig.FILTER_EXPRESSION, SerializationUtil.serializeToBase64(filter));
      } catch (UnsupportedOperationException e) {
        LOG.warn("Unable to create Iceberg filter, continuing without filter (will be applied by Hive later): ", e);
      }
    }

    String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(job);
    job.setStrings(InputFormatConfig.SELECTED_COLUMNS, selectedColumns);
    return job;
  }

  public static CloseableIterable<CombinedScanTask> tasks(JobConf job) {
    return tasks(newJobContext(job));
  }

  public static CloseableIterable<CombinedScanTask> tasks(JobContext context) {
    Configuration conf = context.getConfiguration();
    Table table = deserializeTable(conf);
    TableScan scan = table.newScan()
        .caseSensitive(conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, InputFormatConfig.CASE_SENSITIVE_DEFAULT));
    long snapshotId = conf.getLong(InputFormatConfig.SNAPSHOT_ID, -1);
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }
    long asOfTime = conf.getLong(InputFormatConfig.AS_OF_TIMESTAMP, -1);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }
    long splitSize = conf.getLong(InputFormatConfig.SPLIT_SIZE, 0);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }
    String schemaStr = conf.get(InputFormatConfig.READ_SCHEMA);
    if (schemaStr != null) {
      scan.project(SchemaParser.fromJson(schemaStr));
    }
    String[] selectedColumns = conf.getStrings(InputFormatConfig.SELECTED_COLUMNS);
    if (selectedColumns != null) {
      scan.select(selectedColumns);
    }

    // TODO add a filter parser to get rid of Serialization
    Expression filter = SerializationUtil.deserializeFromBase64(conf.get(InputFormatConfig.FILTER_EXPRESSION));
    if (filter != null) {
      scan = scan.filter(filter);
    }

    return scan.planTasks();
  }

}
