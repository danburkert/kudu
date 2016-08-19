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
package org.apache.kudu.client;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLongs;
import com.sangupta.murmur.Murmur2;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;
import org.apache.kudu.util.ByteVec;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utility class for encoding rows into primary and partition keys.
 */
@InterfaceAudience.Private
class KeyEncoder {

  private final ByteVec buf = ByteVec.create();

  /**
   * Encodes the primary key of the row.
   *
   * @param row the row to encode
   * @return the encoded primary key of the row
   */
  public byte[] encodePrimaryKey(final PartialRow row) {
    buf.clear();
    final Schema schema = row.getSchema();
    for (int columnIdx = 0; columnIdx < schema.getPrimaryKeyColumnCount(); columnIdx++) {
      final boolean isLast = columnIdx + 1 == schema.getPrimaryKeyColumnCount();
      encodeColumn(row, columnIdx, isLast, buf);
    }
    return buf.toArray();
  }

  public static int getHashBucket(PartialRow row, HashBucketSchema hashSchema) {
    ByteVec buf = ByteVec.create();
    encodeColumns(row, hashSchema.getColumnIds(), buf);
    long hash = Murmur2.hash64(buf.data(), buf.len(), hashSchema.getSeed());
    return (int) UnsignedLongs.remainder(hash, hashSchema.getNumBuckets());
  }

  /**
   * Encodes the provided row into a partition key according to the partition schema.
   *
   * @param row the row to encode
   * @param partitionSchema the partition schema describing the table's partitioning
   * @return an encoded partition key
   */
  public byte[] encodePartitionKey(PartialRow row, PartitionSchema partitionSchema) {
    buf.clear();
    if (!partitionSchema.getHashBucketSchemas().isEmpty()) {
      for (final HashBucketSchema hashSchema : partitionSchema.getHashBucketSchemas()) {
        encodeHashBucket(getHashBucket(row, hashSchema), buf);
      }
    }

    encodeColumns(row, partitionSchema.getRangeSchema().getColumns(), buf);
    return buf.toArray();
  }

  /**
   * Encodes the provided row into a range partition key.
   *
   * @param row the row to encode
   * @param rangeSchema the range partition schema
   * @return the encoded range partition key
   */
  public byte[] encodeRangePartitionKey(PartialRow row, PartitionSchema.RangeSchema rangeSchema) {
    buf.clear();
    encodeColumns(row, rangeSchema.getColumns(), buf);
    return buf.toArray();
  }

  /**
   * Encodes a sequence of columns from the row.
   * @param row the row containing the columns to encode
   * @param columnIds the IDs of each column to encode
   */
  private static void encodeColumns(PartialRow row, List<Integer> columnIds, ByteVec buf) {
    for (int i = 0; i < columnIds.size(); i++) {
      boolean isLast = i + 1 == columnIds.size();
      encodeColumn(row, row.getSchema().getColumnIndex(columnIds.get(i)), isLast, buf);
    }
  }

  /**
   * Encodes a single column of a row into the output buffer.
   * @param row the row being encoded
   * @param columnIdx the column index of the column to encode
   * @param isLast whether the column is the last component of the key
   */
  private static void encodeColumn(PartialRow row,
                                   int columnIdx,
                                   boolean isLast,
                                   ByteVec buf) {
    final Schema schema = row.getSchema();
    final ColumnSchema column = schema.getColumnByIndex(columnIdx);
    if (!row.isSet(columnIdx)) {
      throw new IllegalStateException(String.format("Primary key column %s is not set",
                                                    column.getName()));
    }
    final Type type = column.getType();

    if (type == Type.STRING || type == Type.BINARY) {
      encodeBinary(row.getVarLengthData().get(columnIdx), isLast, buf);
    } else {
      encodeSignedInt(row.getRowAlloc(),
                      schema.getColumnOffset(columnIdx),
                      type.getSize(),
                      buf);
    }
  }

  /**
   * Encodes a variable length binary value into the output buffer.
   * @param value the value to encode
   * @param isLast whether the value is the final component in the key
   * @param buf the output buffer
   */
  private static void encodeBinary(ByteBuffer value, boolean isLast, ByteVec buf) {
    value.reset();

    // TODO find a way to not have to read byte-by-byte that doesn't require extra copies. This is
    // especially slow now that users can pass direct byte buffers.
    while (value.hasRemaining()) {
      byte currentByte = value.get();
      buf.push(currentByte);
      if (!isLast && currentByte == 0x00) {
        // If we're a middle component of a composite key, we need to add a \x00
        // at the end in order to separate this component from the next one. However,
        // if we just did that, we'd have issues where a key that actually has
        // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
        // encode \x00 as \x00\x01. -- key_encoder.h
        buf.push((byte) 0x01);
      }
    }

    if (!isLast) {
      buf.push((byte) 0x00);
      buf.push((byte) 0x00);
    }
  }

  /**
   * Encodes a signed integer into the output buffer
   *
   * @param value an array containing the little-endian encoded integer
   * @param offset the offset of the value into the value array
   * @param len the width of the value
   * @param buf the output buffer
   */
  private static void encodeSignedInt(byte[] value,
                                      int offset,
                                      int len,
                                      ByteVec buf) {
    // Picking the first byte because big endian.
    byte lastByte = value[offset + (len - 1)];
    lastByte = Bytes.xorLeftMostBit(lastByte);
    buf.push(lastByte);
    if (len > 1) {
      for (int i = len - 2; i >= 0; i--) {
        buf.push(value[offset + i]);
      }
    }
  }

  /**
   * Encodes a hash bucket into the buffer.
   * @param bucket the bucket
   * @param buf the buffer
   */
  public static void encodeHashBucket(int bucket, ByteVec buf) {
    buf.append(Ints.toByteArray(bucket));
  }
}
