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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.sangupta.murmur.Murmur2;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.ColumnSchema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduPredicate.ComparisonOp;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.PleaseThrottleException;
import org.kududb.client.RowError;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@ThreadSafe
public class TagsetCache {
  private static final Logger LOG = LoggerFactory.getLogger(TagsetCache.class);

  private final AsyncKuduClient client;
  private final KuduTable tagsetsTable;
  private final KuduTable tagsTable;
  private final ColumnSchema tagsetIDColumn;
  private final int tagsetIDColumnIndex;
  private final int tagsetColumnIndex;
  private final int tagsKeyColumnIndex;
  private final int tagsValueColumnIndex;
  private final int tagsTagsetIDColumnIndex;
  private final List<Integer> columnIndexes;

  private static final long SEED = 0X9883143CFDDB5C3FL;

  /** Map of tagset to tagset ID. */
  private final LoadingCache<ByteBuffer, Deferred<Long>> tagsets;

  public TagsetCache(AsyncKuduClient client, KuduTSSchema schema,
                     KuduTable tagsetsTable, KuduTable tagsTable) {
    this.client = client;
    this.tagsetsTable = tagsetsTable;
    this.tagsTable = tagsTable;
    this.tagsetIDColumnIndex = schema.getTagsetsSchema().getColumnIndex("id");
    this.tagsetColumnIndex = schema.getTagsetsSchema().getColumnIndex("tagset");
    this.columnIndexes = ImmutableList.of(tagsetIDColumnIndex, tagsetColumnIndex);
    this.tagsetIDColumn = schema.getTagsetsSchema().getColumnByIndex(tagsetIDColumnIndex);
    this.tagsKeyColumnIndex = schema.getTagsSchema().getColumnIndex("key");
    this.tagsValueColumnIndex = schema.getTagsSchema().getColumnIndex("value");
    this.tagsTagsetIDColumnIndex = schema.getTagsSchema().getColumnIndex("id");
    this.tagsets = CacheBuilder.newBuilder()
                               .maximumSize(1024 * 1024)
                               .build(new CacheLoader<ByteBuffer, Deferred<Long>>() {
                                 @Override
                                 public Deferred<Long> load(ByteBuffer tagset) throws Exception {
                                   return lookupTagsetID(tagset);
                                 }
                               });
  }

  /**
   * Get the ID for a tagset. If the tagset doesn't already have an assigned ID,
   * then a new ID entry will be inserted into the {@code tagset} table, and new
   * tag entries added to the {@code tags} table.
   * @param tagset the tagset
   * @return the ID for the tagset
   */
  public Deferred<Long> getTagsetID(SortedMap<String, String> tagset) {
    return tagsets.getUnchecked(serializeTagset(tagset));
  }

  @VisibleForTesting
  void clear() {
    tagsets.invalidateAll();
  }

  @VisibleForTesting
  static ByteBuffer serializeTagset(SortedMap<String, String> tagset) {
    Messages.Tagset.Builder builder = Messages.Tagset.newBuilder();
    for (Map.Entry<String, String> tag : tagset.entrySet()) {
      builder.addTagsBuilder().setKey(tag.getKey()).setValue(tag.getValue());
    }

    try {
      Messages.Tagset pb = builder.build();
      ByteArrayOutputStream baos = new ByteArrayOutputStream(pb.getSerializedSize());
      pb.writeTo(baos);
      byte[] bytes = baos.toByteArray();
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      return buf;
    } catch (IOException e) {
      // This should be impossible with ByteArrayOutputStream
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  static long hashSerializedTagset(ByteBuffer tagset) {
    if (!tagset.hasArray()) {
      throw new IllegalArgumentException("serialized tagset ByteBuffer must have an array");
    }
    return Murmur2.hash64(tagset.array(), tagset.position(), SEED);
  }

  /**
   * Reads the ID of a tagset from the {@code tagset} table. If the tagset
   * doesn't exist in the table, then it is added along with corresponding
   * entries in the {@code tags} table.
   *
   * @param tagset the serialized tagset
   * @return the tagset ID
   */
  private Deferred<Long> lookupTagsetID(final ByteBuffer tagset) {
    final long hash = hashSerializedTagset(tagset);
    return lookupTagset(tagset, hash).addCallbackDeferring(
        new Callback<Deferred<Long>, TagsetLookupResult>() {
          @Override
          public Deferred<Long> call(TagsetLookupResult result) throws Exception {
            if (result.isFound()) {
              return Deferred.fromResult(result.id);
            } else {
              return insertTagset(tagset, hash).addCallbackDeferring(new InsertTags(tagset));
            }
          }
        });
  }

  /**
   * Creates an {@link AsyncKuduScanner} over the tagset table beginning with the specified ID.
   * @param from the ID to begin scanning from
   * @return the scanner
   */
  private AsyncKuduScanner tagsetScanner(long from) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scanBuilder = client.newScannerBuilder(tagsetsTable);
    scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetIDColumn,
                                                                  ComparisonOp.GREATER_EQUAL,
                                                                  from));
    if (from < Long.MAX_VALUE - 10) {
      scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetIDColumn,
                                                                    ComparisonOp.LESS,
                                                                    from + 10));
    }
    scanBuilder.setProjectedColumnIndexes(columnIndexes);
    return scanBuilder.build();
  }

  private Deferred<TagsetLookupResult> lookupTagset(ByteBuffer tagset, long hash) {
    AsyncKuduScanner tagsetScanner = tagsetScanner(hash);

    return tagsetScanner.nextRows().addCallbackDeferring(
        new TagsetLookup(tagset, hash, tagsetScanner));
  }

  /**
   * Attempts to insert the provided tagset and ID.
   * @param tagset the tagset to insert
   * @param id the ID to insert the tagset with
   * @return the completion
   */
  private Deferred<OperationResponse> insertTagsetWithID(final ByteBuffer tagset, final long id) {
    final AsyncKuduSession session = client.newSession();
    Insert insert = tagsetsTable.newInsert();
    insert.getRow().addLong(tagsetIDColumnIndex, id);
    insert.getRow().addBinary(tagsetColumnIndex, tagset);
    try {
      return session.apply(insert);
    } catch (PleaseThrottleException e) {
      return e.getDeferred().addCallbackDeferring(new Callback<Deferred<OperationResponse>, Object>() {
        @Override
        public Deferred<OperationResponse> call(Object obj) throws Exception {
          return insertTagsetWithID(tagset, id);
        }
      });
    }
  }

  /**
   * Inserts the tagset into the {@code tagset} table, and returns the tagset ID.
   * @param tagset the tagset
   * @param hash the hash of the tagset
   * @return the tagset ID
   */
  private Deferred<Long> insertTagset(ByteBuffer tagset, long hash) {
    return insertTagsetWithID(tagset, hash).addCallbackDeferring(new TagsetInsert(tagset, hash));
  }

  private static class TagsetLookupResult {
    private final long id;
    private final boolean found;

    private TagsetLookupResult(boolean found, long id) {
      this.found = found;
      this.id = id;
    }

    public boolean isFound() {
      return found;
    }

    public static TagsetLookupResult found(long id) {
      return new TagsetLookupResult(true, id);
    }

    public static TagsetLookupResult notFound(long id) {
      return new TagsetLookupResult(false, id);
    }
  }

  private final class TagsetLookup implements Callback<Deferred<TagsetLookupResult>, RowResultIterator> {
    private final ByteBuffer tagset;
    private AsyncKuduScanner scanner;
    private final long hash;
    private long probe;

    public TagsetLookup(ByteBuffer tagset, long hash, AsyncKuduScanner scanner) {
      this.tagset = tagset;
      this.scanner = scanner;
      this.hash = hash;
      this.probe = hash;
    }

    @Override
    public Deferred<TagsetLookupResult> call(RowResultIterator rows) throws Exception {
      for (RowResult row : rows) {
        long id = row.getLong(tagsetIDColumnIndex);
        if (id != probe) {
          // We didn't find the tagset
          return Deferred.fromResult(TagsetLookupResult.notFound(probe + 1));
        }

        if (row.getBinary(tagsetColumnIndex).equals(tagset)) {
          return Deferred.fromResult(TagsetLookupResult.found(id));
        }

        probe++;
      }

      // We probed through the entire RowResult and didn't find the tagset.

      if (!scanner.hasMoreRows()) {
        if (hash == probe) {
          // The scan returned no results
          return Deferred.fromResult(TagsetLookupResult.notFound(probe));
        }
        scanner = tagsetScanner(probe);
      }
      return scanner.nextRows().addCallbackDeferring(this);
    }
  }

  private final class TagsetInsert implements Callback<Deferred<Long>, OperationResponse> {
    private final ByteBuffer tagset;
    private long id;

    public TagsetInsert(ByteBuffer tagset, long id) {
      this.tagset = tagset;
      this.id = id;
    }

    @Override
    public Deferred<Long> call(OperationResponse response) throws Exception {
      if (response.hasRowError()) {
        RowError error = response.getRowError();
        if (error.getErrorStatus().isAlreadyPresent()) {
          // Try again at an incremented ID value.
          id++;

          return insertTagsetWithID(tagset, id).addCallbackDeferring(this);
        } else {
          return Deferred.fromError(new RuntimeException(String.format("Unable to insert tagset: %s",
                                                                       error.toString())));
        }
      }
      return Deferred.fromResult(id);
    }
  }

  private final class InsertTags implements Callback<Deferred<Long>, Long> {
    Messages.Tagset tagset;

    public InsertTags(ByteBuffer tagset) {
      if (!tagset.hasArray()) {
        throw new IllegalArgumentException("serialized tagset ByteBuffer must have an array");
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(tagset.array(),
                                                           tagset.position(),
                                                           tagset.limit() - tagset.position());
      try {
        this.tagset = Messages.Tagset.parseFrom(bais);
      } catch (IOException e) {
        // Should never happen with ByteArrayInputStream.
        throw new RuntimeException(e);
      }
    }

    @Override
    public Deferred<Long> call(final Long id) throws Exception {
      AsyncKuduSession session = client.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      for (Messages.Tagset.Tag tag : tagset.getTagsList()) {
        Insert insert = tagsTable.newInsert();
        insert.getRow().addString(tagsKeyColumnIndex, tag.getKey());
        insert.getRow().addString(tagsValueColumnIndex, tag.getValue());
        insert.getRow().addLong(tagsTagsetIDColumnIndex, id);
        // TODO: check with JD if the below fails, it will be caught in the flush
        session.apply(insert);
      }

      return session.flush().addCallbackDeferring(new Callback<Deferred<Long>,
                                                  List<OperationResponse>>() {
        @Override
        public Deferred<Long> call(List<OperationResponse> responses) throws Exception {
          for (OperationResponse response : responses) {
            if (response.hasRowError()) {
              return Deferred.fromError(new RuntimeException(
                  String.format("Unable to insert tag: %s", response.getRowError())));
            }
          }
          return Deferred.fromResult(id);
        }
      });
    }
  }
}
