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

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class T {
  private T() {
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.hadoop." + METASTOREURIS.varname, "thrift://localhost:9083")
        .config("spark.executor.heartbeatInterval", "100000")
        .config("spark.network.timeoutInterval", "100000")
        .enableHiveSupport()
        .getOrCreate();

    // createTable(spark);
    write(spark);
    read(spark);

  }

  private static void read(SparkSession spark) {
    spark.read()
        .format("iceberg")
        .load("default.person")
        .show(false);
  }

  private static void createTable(SparkSession spark) {
    TableIdentifier name = TableIdentifier.of("default", "person");

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
        Types.NestedField.required(3, "age", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Catalog catalog = new HiveCatalog(spark.sparkContext().hadoopConfiguration());
    Table table = catalog.createTable(name, schema, spec);
    System.out.println(table.location());
  }

  private static void write0(SparkSession spark) {
    List<Person> list = Lists.newArrayList(new Person(1, "iteblog", 100), new Person(2, "iteblog2", 300));
    Encoder<Person> personEncoder = Encoders.bean(Person.class);

    Dataset<Person> javaBeanDS = spark.createDataset(list, personEncoder);
    javaBeanDS.write().format("iceberg").mode("append").save("default.person");
  }

  private static void write(SparkSession spark) {
    List<Person> list = Lists.newArrayList(new Person(1, "iteblog", 100), new Person(2, "iteblog2", 300));
    JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
    JavaRDD<Row> rowRDD = javaSparkContext.parallelize(list)
        .map((Function<Person, Row>) record -> RowFactory.create(record.getId(), record.getName(), record.getAge()));

    StructType structType = new StructType()
        .add("id", "int", false)
        .add("name", "string", true)
        .add("age", "int", false);

    Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, structType);

    dataFrame.write().format("iceberg").mode("append").save("default.person");
  }
}
