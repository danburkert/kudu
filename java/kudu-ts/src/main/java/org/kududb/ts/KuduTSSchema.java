/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.kududb.ts;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTSSchema {
  private final String metricsTableName;
  private final String tagsetsTableName;
  private final String tagsTableName;

  private final Schema metricsSchema;
  private final Schema tagsetsSchema;
  private final Schema tagsSchema;

  public static KuduTSSchema create(String tableName) {

    Schema metricsSchema = new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("tagset_id", Type.INT64).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("time", Type.TIMESTAMP).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).nullable(false).build()));

    Schema tagsetsSchema = new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("tagset", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).nullable(false).build()));

    Schema tagsSchema = new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).build()));

    return new KuduTSSchema(tableName, metricsSchema, tagsetsSchema, tagsSchema);
  }

  public Schema getMetricsSchema() {
    return metricsSchema;
  }

  public Schema getTagsetsSchema() {
    return tagsetsSchema;
  }

  public Schema getTagsSchema() {
    return tagsSchema;
  }

  public String getMetricsTableName() {
    return metricsTableName;
  }

  public String getTagsetsTableName() {
    return tagsetsTableName;
  }

  public String getTagsTableName() {
    return tagsTableName;
  }

  public static String metricsTableName(String tableName) {
    return String.format("kuduts.%s.metrics", tableName);
  }

  public static String tagsetsTableName(String tableName) {
    return String.format("kuduts.%s.tagsets", tableName);
  }

  public static String tagsTableName(String tableName) {
    return String.format("kuduts.%s.tags", tableName);
  }

  private static Schema validateMetricsSchema(Schema metricsSchema) {
    // TODO: validate
    return metricsSchema;
  }

  private static Schema validateTagsetsSchema(Schema tagsetsSchema) {
    // TODO: validate
    return tagsetsSchema;
  }

  private static Schema validateTagsSchema(Schema tagsSchema) {
    // TODO: validate
    return tagsSchema;
  }

  KuduTSSchema(String tableName, Schema metricsSchema, Schema tagsetsSchema, Schema tagsSchema) {
    this.metricsTableName = metricsTableName(tableName);
    this.tagsetsTableName = tagsetsTableName(tableName);
    this.tagsTableName = tagsTableName(tableName);
    this.metricsSchema = validateMetricsSchema(metricsSchema);
    this.tagsetsSchema = validateTagsetsSchema(tagsetsSchema);
    this.tagsSchema = validateTagsSchema(tagsSchema);
  }
}
