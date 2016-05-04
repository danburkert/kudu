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

/**
 * {@code Tables} holds meta information about the table schemas used by {@code KuduTS}.
 */
@InterfaceAudience.Private
class Tables {

  private Tables() {}

  static final int METRICS_METRIC_INDEX = 0;
  static final int METRICS_TAGSET_ID_INDEX = 1;
  static final int METRICS_TIME_INDEX = 2;
  static final int METRICS_VALUE_INDEX = 3;

  static final int TAGSETS_ID_INDEX = 0;
  static final int TAGSETS_TAGSET_INDEX = 1;

  static final int TAGS_KEY_INDEX = 0;
  static final int TAGS_VALUE_INDEX = 1;
  static final int TAGS_TAGSET_ID_INDEX = 2;

  static final ColumnSchema METRICS_METRIC_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).nullable(false).key(true).build();
  static final ColumnSchema METRICS_TAGSET_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset_id", Type.INT32).nullable(false).key(true).build();
  static final ColumnSchema METRICS_TIME_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("time", Type.TIMESTAMP).nullable(false).key(true).build();
  static final ColumnSchema METRICS_VALUE_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).nullable(false).build();

  static final ColumnSchema TAGSETS_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).nullable(false).key(true).build();
  static final ColumnSchema TAGSETS_TAGSET_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset", Type.BINARY).nullable(false).build();

  static final ColumnSchema TAGS_KEY_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).nullable(false).key(true).build();
  static final ColumnSchema TAGS_VALUE_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(false).key(true).build();
  static final ColumnSchema TAGS_TAGSET_ID_COLUMN =
      new ColumnSchema.ColumnSchemaBuilder("tagset_id", Type.INT32).nullable(false).key(true).build();

  static final Schema METRICS_SCHEMA = new Schema(ImmutableList.of(METRICS_METRIC_COLUMN,
                                                                   METRICS_TAGSET_ID_COLUMN,
                                                                   METRICS_TIME_COLUMN,
                                                                   METRICS_VALUE_COLUMN));
  static final Schema TAGSETS_SCHEMA = new Schema(ImmutableList.of(TAGSETS_ID_COLUMN,
                                                                   TAGSETS_TAGSET_COLUMN));
  static final Schema TAGS_SCHEMA = new Schema(ImmutableList.of(TAGS_KEY_COLUMN,
                                                                TAGS_VALUE_COLUMN,
                                                                TAGS_TAGSET_ID_COLUMN));

  static String metricsTableName(String tsName) {
    return String.format("kuduts.%s.metrics", tsName);
  }

  static String tagsetsTableName(String tsName) {
    return String.format("kuduts.%s.tagsets", tsName);
  }

  static String tagsTableName(String tsName) {
    return String.format("kuduts.%s.tags", tsName);
  }
}
