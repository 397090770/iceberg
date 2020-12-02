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

package org.apache.iceberg.iteblog;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.iteblog.TableTest.findTable;

/**
 * @author https://www.iteblog.com
 * @version 2020-11-30 10:28
 */
public class RemoveOrphanFilesActionTest {
  private RemoveOrphanFilesActionTest() {
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.set(METASTOREURIS.varname, "thrift://localhost:9083");

    Map<String, String> maps = Maps.newHashMap();
    maps.put("path", "default.iteblog");
    DataSourceOptions options = new DataSourceOptions(maps);

    Table table = findTable(options, conf);

    SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, "thrift://localhost:9083")
        .config("spark.executor.heartbeatInterval", "100000")
        .config("spark.network.timeoutInterval", "100000")
        .enableHiveSupport()
        .getOrCreate();

    Actions.forTable(table)
        .removeOrphanFiles()
        .execute();
  }
}
