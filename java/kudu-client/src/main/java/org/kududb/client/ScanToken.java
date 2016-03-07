// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.kududb.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ZeroCopyLiteralByteString;
import org.kududb.ColumnSchema;
import org.kududb.Common;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.Client.ScanTokenPB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A scan token describes a partial scan of a Kudu table limited to a single
 * contiguous physical location. Using the {@link ScanTokenBuilder}, client can
 * describe the desired scan, including predicates, bounds, timestamps, and
 * caching, and receive back a collection of scan tokens.
 *
 * Each scan token may be separately turned into a scanner using
 * {@link #intoScanner}, with each scanner responsible for a disjoint section
 * of the table.
 *
 * Scan tokens may be serialized using the {@link #serialize} method and
 * deserialized back into a scanner using the {@link #deserializeIntoScanner}
 * method. This allows scan tokens to be created in a context separate of the
 * scan execution.
 *
 * Scan token locality information can be inspected using the {@link #getTablet}
 * method.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ScanToken implements Comparable<ScanToken> {
  final LocatedTablet tablet;
  final ScanTokenPB message;

  private ScanToken(LocatedTablet tablet, ScanTokenPB message) {
    this.tablet = tablet;
    this.message = message;
  }

  /**
   * Returns the tablet which the scanner created from this token will access.
   * @return the located tablet
   */
  public LocatedTablet getTablet() {
    return tablet;
  }

  /**
   * Creates a {@link KuduScanner} from this scan token.
   * @param client a Kudu client for the cluster
   * @return a scanner for the scan token
   */
  public KuduScanner intoScanner(KuduClient client) throws Exception {
    return pbIntoScanner(message, client);
  }

  /**
   * Serializes this {@code ScanToken} into a byte buffer.
   * @return the serialized scan token
   * @throws IOException
   */
  public ByteBuffer serialize() throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(message.getSerializedSize());
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    message.writeTo(cos);
    cos.flush();
    buf.flip();
    return buf;
  }

  /**
   * Deserializes a {@code ScanToken} into a {@link KuduScanner}.
   * @param buf a buffer containing the serialized scan token.
   * @param client a Kudu client for the cluster
   * @return a scanner for the serialized scan token
   * @throws Exception
   */
  public static KuduScanner deserializeIntoScanner(ByteBuffer buf,
                                                   KuduClient client) throws Exception {
    return pbIntoScanner(ScanTokenPB.parseFrom(CodedInputStream.newInstance(buf)), client);
  }

  private static KuduScanner pbIntoScanner(ScanTokenPB message,
                                           KuduClient client) throws Exception {
    Preconditions.checkArgument(
        !message.getFeatureFlagsList().contains(ScanTokenPB.Features.Unknown),
        "Scan token requires an unsupported feature. This Kudu client must be updated.");

    KuduTable table = client.openTable(message.getTableName());
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);

    List<Integer> columns = new ArrayList<>(message.getProjectedColumnsCount());
    for (Common.ColumnSchemaPB column : message.getProjectedColumnsList()) {
      int columnIdx = table.getSchema().getColumnIndex(column.getName());
      ColumnSchema schema = table.getSchema().getColumnByIndex(columnIdx);
      Preconditions.checkArgument(column.getType() == schema.getType().getDataType(),
                                  String.format("Column types do not match for column %s",
                                                column.getName()));
      columns.add(columnIdx);
    }
    builder.setProjectedColumnIndexes(columns);

    for (Common.ColumnPredicatePB pred : message.getColumnPredicatesList()) {
      builder.addPredicate(KuduPredicate.fromPB(table.getSchema(), pred));
    }

    if (message.hasLowerBoundPrimaryKey()) {
      builder.lowerBoundRaw(message.getLowerBoundPrimaryKey().toByteArray());
    }
    if (message.hasUpperBoundPrimaryKey()) {
      builder.exclusiveUpperBoundRaw(message.getUpperBoundPrimaryKey().toByteArray());
    }

    if (message.hasLowerBoundPartitionKey()) {
      builder.lowerBoundPartitionKeyRaw(message.getLowerBoundPartitionKey().toByteArray());
    }
    if (message.hasUpperBoundPartitionKey()) {
      builder.exclusiveUpperBoundPartitionKeyRaw(message.getUpperBoundPartitionKey().toByteArray());
    }

    if (message.hasLimit()) {
      builder.limit(message.getLimit());
    }

    if (message.hasReadMode()) {
      switch (message.getReadMode()) {
        case READ_AT_SNAPSHOT: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
          if (message.hasSnapTimestamp()) {
            builder.snapshotTimestampRaw(message.getSnapTimestamp());
          }
          break;
        }
        case READ_LATEST: {
          builder.readMode(AsyncKuduScanner.ReadMode.READ_LATEST);
          break;
        }
        default: throw new IllegalArgumentException("unknown read mode");
      }
    }

    if (message.hasPropagatedTimestamp()) {
      // TODO
    }

    if (message.hasCacheBlocks()) {
      builder.cacheBlocks(message.getCacheBlocks());
    }

    if (message.hasOrderMode()) {
      // TODO
    }
    return builder.build();
  }

  @Override
  public int compareTo(ScanToken other) {
    if (!message.getTableName().equals(other.message.getTableName())) {
      throw new IllegalArgumentException("Scan tokens from different builders may not be compared");
    }

    return tablet.getPartition().compareTo(other.getTablet().getPartition());
  }

  /**
   * Builds a sequence of scan tokens.
   */
  public static class ScanTokenBuilder
      extends AbstractKuduScannerBuilder<ScanTokenBuilder, List<ScanToken>> {

    private long timeout;

    ScanTokenBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
      timeout = client.getDefaultOperationTimeoutMs();
    }

    /**
     * Sets a timeout value to use when building the list of scan tokens. If
     * unset, the client operation timeout will be used.
     * @param timeoutMs the timeout in milliseconds.
     */
    public ScanTokenBuilder setTimeout(long timeoutMs) {
      timeout = timeoutMs;
      return this;
    }

    @Override
    public List<ScanToken> build() {
      if (lowerBoundPartitionKey != AsyncKuduClient.EMPTY_ARRAY ||
          upperBoundPartitionKey != AsyncKuduClient.EMPTY_ARRAY) {
        throw new IllegalArgumentException(
            "Partition key bounds may not be set on ScanTokenBuilder");
      }

      // If the scan is short-circuitable, then return no tokens.
      for (KuduPredicate predicate : this.predicates.values()) {
        if (predicate.getType() == KuduPredicate.PredicateType.NONE) {
          return ImmutableList.of();
        }
      }

      Client.ScanTokenPB.Builder proto = Client.ScanTokenPB.newBuilder();

      proto.setTableName(table.getName());

      // Map the column names or indices to actual columns in the table schema.
      // If the user did not set either projection, then scan all columns.
      if (projectedColumnNames != null) {
        for (String columnName : projectedColumnNames) {
          ColumnSchema columnSchema = table.getSchema().getColumn(columnName);
          Preconditions.checkArgument(columnSchema != null, "unknown column %s", columnName);
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), columnSchema);
        }
      } else if (projectedColumnIndexes != null) {
        for (int columnIdx : projectedColumnIndexes) {
          ColumnSchema columnSchema = table.getSchema().getColumnByIndex(columnIdx);
          Preconditions.checkArgument(columnSchema != null, "unknown column index %s", columnIdx);
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), columnSchema);
        }
      } else {
        for (ColumnSchema column : table.getSchema().getColumns()) {
          ProtobufHelper.columnToPb(proto.addProjectedColumnsBuilder(), column);
        }
      }

      for (KuduPredicate predicate : predicates.values()) {
        proto.addColumnPredicates(predicate.toPB());
      }

      if (lowerBoundPrimaryKey != AsyncKuduClient.EMPTY_ARRAY && lowerBoundPrimaryKey.length > 0) {
        proto.setLowerBoundPrimaryKey(ZeroCopyLiteralByteString.copyFrom(lowerBoundPrimaryKey));
      }
      if (upperBoundPrimaryKey != AsyncKuduClient.EMPTY_ARRAY && upperBoundPrimaryKey.length > 0) {
        proto.setUpperBoundPrimaryKey(ZeroCopyLiteralByteString.copyFrom(upperBoundPrimaryKey));
      }
      if (lowerBoundPartitionKey != AsyncKuduClient.EMPTY_ARRAY &&
          lowerBoundPartitionKey.length > 0) {
        proto.setLowerBoundPartitionKey(ZeroCopyLiteralByteString.copyFrom(lowerBoundPartitionKey));
      }
      if (upperBoundPartitionKey != AsyncKuduClient.EMPTY_ARRAY &&
          upperBoundPartitionKey.length > 0) {
        proto.setUpperBoundPartitionKey(ZeroCopyLiteralByteString.copyFrom(upperBoundPartitionKey));
      }

      proto.setLimit(limit);
      proto.setReadMode(readMode.pbVersion());

      // if the last propagated timestamp is set send it with the scan
      if (table.getAsyncClient().getLastPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
        proto.setPropagatedTimestamp(client.getLastPropagatedTimestamp());
      }

      // if the mode is set to read on snapshot sent the snapshot timestamp
      if (readMode == AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT &&
          htTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
        proto.setSnapTimestamp(htTimestamp);
      }

      proto.setCacheBlocks(cacheBlocks);

      try {
        List<LocatedTablet> tablets;
        if (table.getPartitionSchema().isSimpleRangePartitioning()) {
          // TODO: replace this with proper partition pruning
          tablets = table.getTabletsLocations(
              lowerBoundPrimaryKey.length == 0 ? null : lowerBoundPrimaryKey,
              upperBoundPrimaryKey.length == 0 ? null : upperBoundPrimaryKey,
              timeout);
        } else {
          tablets = table.getTabletsLocations(timeout);
        }

        List<ScanToken> tokens = new ArrayList<>(tablets.size());
        for (LocatedTablet tablet : tablets) {
          Client.ScanTokenPB.Builder builder = proto.clone();
          builder.setLowerBoundPartitionKey(
              ZeroCopyLiteralByteString.wrap(tablet.getPartition().partitionKeyStart));
          builder.setUpperBoundPartitionKey(
              ZeroCopyLiteralByteString.wrap(tablet.getPartition().partitionKeyEnd));
          tokens.add(new ScanToken(tablet, builder.build()));
        }
        return tokens;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
 }
