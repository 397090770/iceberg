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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class TableTest {
  private TableTest() {
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.set(METASTOREURIS.varname, "thrift://localhost:9083");

    Map<String, String> maps = Maps.newHashMap();
    maps.put("path", "default.iteblog");
    DataSourceOptions options = new DataSourceOptions(maps);

    Table table = findTable(options, conf);

    Snapshot snapshot = table.currentSnapshot();

    snapshot.dataManifests().forEach(manifestFile -> System.out.println(manifestFile.path()));

    System.out.println(snapshot.parentId() + ", " + snapshot.snapshotId());
    System.out.println(snapshot.allManifests().size());
    snapshot.allManifests().forEach(manifestFile -> System.out.println(manifestFile.path()));


  }

  static Table findTable(DataSourceOptions options, Configuration conf) {
    Optional<String> path = options.get("path");
    Preconditions.checkArgument(path.isPresent(), "Cannot open table: path is not set");

    if (path.get().contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path.get());
    } else {
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path.get());
      return hiveCatalog.loadTable(tableIdentifier);
    }
  }
}