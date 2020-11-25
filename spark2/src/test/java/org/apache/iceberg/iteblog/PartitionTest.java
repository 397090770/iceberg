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

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;

public class PartitionTest {
  private PartitionTest() {
  }

  private static String databaseName = "default";
  private static String tableName = "iteblog1";
  private static String fullName = databaseName + "." + tableName;

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, "thrift://localhost:9083")
        .config("spark.hadoop." + METASTOREWAREHOUSE.varname, "/data/hive/warehouse")
        .config("spark.executor.heartbeatInterval", "100000")
        .config("spark.network.timeoutInterval", "100000")
        .enableHiveSupport()
        .getOrCreate();

    // createTableIfNotExist(spark);
    write(spark);
    read(spark);
  }

  private static void read(SparkSession spark) {
    spark.read()
        .format("iceberg")
        .load(fullName)
        .show(false);
  }

  private static void createTableIfNotExist(SparkSession spark) {
    TableIdentifier name = TableIdentifier.of(databaseName, tableName);

    Catalog catalog = new HiveCatalog(spark.sparkContext().hadoopConfiguration());
    // catalog.dropTable(name);
    // System.exit(127);
    boolean exists = catalog.tableExists(name);

    if (!exists) {
      Schema schema = new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()),
          Types.NestedField.optional(4, "ts", Types.TimestampType.withZone())
      );

      PartitionSpec spec = PartitionSpec.builderFor(schema).year("ts").bucket("id", 2).build();

      Table table = catalog.createTable(name, schema, spec);
      System.out.println(table.location());
    }
  }

  private static void write(SparkSession spark) {

    List<Person> list = Stream.iterate(1, x -> ++x).limit(1000000)
        .map(i -> new Person(0, UUID.randomUUID().toString(), i, Timestamp.valueOf("2020-11-01 00:00:00")))
        .collect(Collectors.toList());

//    List<Person> list = Lists.newArrayList(new Person(1, "iteblog1", 100, Timestamp.valueOf("2020-11-01 00:00:00")),
//        new Person(2, "iteblog2", 300, Timestamp.valueOf("2020-11-01 00:00:00")),
//        new Person(3, "iteblog", 100, Timestamp.valueOf("2020-12-02 00:00:00")),
//        new Person(4, "iteblog", 100, Timestamp.valueOf("2020-11-02 00:00:00")));
    JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = javaSparkContext.parallelize(list)
        .map((Function<Person, Row>) record ->
            RowFactory.create(record.getId(), record.getName(), record.getAge(), record.getTs()));

    StructType structType = new StructType()
        .add("id", "int", false)
        .add("name", "string", true)
        .add("age", "int", false)
        .add("ts", "timestamp", true);

    Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, structType);

    dataFrame.write().format("iceberg").mode("append").save(fullName);
  }
}
