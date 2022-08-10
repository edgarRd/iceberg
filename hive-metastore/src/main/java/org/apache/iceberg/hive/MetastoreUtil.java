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
package org.apache.iceberg.hive;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class MetastoreUtil {

  private static final DynMethods.UnboundMethod ALTER_TABLE =
      DynMethods.builder("alter_table")
          .impl(
              IMetaStoreClient.class,
              "alter_table_with_environmentContext",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(
              IMetaStoreClient.class,
              "alter_table",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(IMetaStoreClient.class, "alter_table", String.class, String.class, Table.class)
          .build();

  private MetastoreUtil() {}

  /**
   * Calls alter_table method using the metastore client. If possible, an environmental context will
   * be used that turns off stats updates to avoid recursive listing.
   */
  public static void alterTable(
      IMetaStoreClient client, String databaseName, String tblName, Table table) {
    EnvironmentContext envContext =
        new EnvironmentContext(
            ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE));
    ALTER_TABLE.invoke(client, databaseName, tblName, table, envContext);
  }
}
