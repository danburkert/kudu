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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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
import java.util.ArrayList;
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
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@ThreadSafe
class TagsetCache {
  private static final Logger LOG = LoggerFactory.getLogger(TagsetCache.class);

  // Number of tags to return per tagset scanner.
  private int TAGS_PER_SCAN = 10;

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

  TagsetCache(AsyncKuduClient client, KuduTSSchema schema,
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
                                   final long hash = hashSerializedTagset(tagset);
                                   return lookupOrInsertTagset(tagset, hash);
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
  Deferred<Long> getTagsetID(SortedMap<String, String> tagset) {
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
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      // This should be impossible with ByteArrayOutputStream
      throw new RuntimeException(e);
    }
  }

  private static Messages.Tagset deserializeTagset(ByteBuffer tagset) {
    if (!tagset.hasArray()) {
      throw new IllegalArgumentException("serialized tagset ByteBuffer must have an array");
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(tagset.array(),
                                                         tagset.position(),
                                                         tagset.limit() - tagset.position());
    try {
      return Messages.Tagset.parseFrom(bais);
    } catch (IOException e) {
      // Should never happen with ByteArrayInputStream.
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
  private Deferred<Long> lookupOrInsertTagset(final ByteBuffer tagset, final long id) {
    Callback<Deferred<Long>, TagsetLookupResult> lookupResultCB = new Callback<Deferred<Long>, TagsetLookupResult>() {
      @Override
      public Deferred<Long> call(TagsetLookupResult result) throws Exception {
        if (result.isFound()) {
          return Deferred.fromResult(result.id);
        } else {
          final long probe = result.id;
          return insertTagset(tagset, probe).addCallbackDeferring(new Callback<Deferred<Long>, Boolean>() {
            @Override
            public Deferred<Long> call(Boolean success) throws Exception {
              if (success) {
                return insertTags(tagset, probe);
              } else {
                return lookupOrInsertTagset(tagset, probe);
              }
            }
          });
        }
      }
    };

    return lookupTagset(tagset, id).addCallbackDeferring(lookupResultCB);
  }

  private Deferred<TagsetLookupResult> lookupTagset(ByteBuffer tagset, long id) {
    LOG.info("looking up tagset: {}, id: {}", deserializeTagset(tagset), id);
    AsyncKuduScanner tagsetScanner = tagsetScanner(id);

    return tagsetScanner.nextRows().addCallbackDeferring(
        new TagsetLookupCB(tagset, id, tagsetScanner));
  }

  /**
   * Creates an {@link AsyncKuduScanner} over the tagset table beginning with
   * the specified ID.
   * @param id the ID to begin scanning from
   * @return the scanner
   */
  private AsyncKuduScanner tagsetScanner(long id) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scanBuilder = client.newScannerBuilder(tagsetsTable);
    scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetIDColumn,
                                                                  ComparisonOp.GREATER_EQUAL,
                                                                  id));
    if (id < Long.MAX_VALUE - TAGS_PER_SCAN) {
      scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetIDColumn,
                                                                    ComparisonOp.LESS,
                                                                    id + TAGS_PER_SCAN));
    }
    scanBuilder.setProjectedColumnIndexes(columnIndexes);
    return scanBuilder.build();
  }

  /**
   * Attempts to insert the provided tagset and ID. Returns {@code true} if the
   * write was successful, or {@code false} if the write failed due to a tagset
   * with the same ID already existing in the table.
   * @param tagset the tagset to insert
   * @param id the ID to insert the tagset with
   * @return whether the write succeeded
   */
  private Deferred<Boolean> insertTagset(final ByteBuffer tagset, final long id) {
    LOG.debug("Inserting tagset: {}", deserializeTagset(tagset));
    final AsyncKuduSession session = client.newSession();
    final Insert insert = tagsetsTable.newInsert();
    insert.getRow().addLong(tagsetIDColumnIndex, id);
    insert.getRow().addBinary(tagsetColumnIndex, tagset);
    return Deferred.fromResult(new Object())
        .addCallbackDeferring(new Callback<Deferred<OperationResponse>, Object>() {
          @Override
          public Deferred<OperationResponse> call(Object obj) throws Exception {
            try {
              return session.apply(insert);
            } catch (PleaseThrottleException e) {
              LOG.warn("Throttling tagset insert", e);
              return e.getDeferred().addCallbackDeferring(this);
            }
          }
        })
        .addCallbackDeferring(new Callback<Deferred<Boolean>, OperationResponse>() {
          @Override
          public Deferred<Boolean> call(OperationResponse response) throws Exception {
            if (response.hasRowError()) {
              if (response.getRowError().getErrorStatus().isAlreadyPresent()) {
                LOG.info("Attempted to insert duplicate tagset ID: {}", response.getRowError());
                return Deferred.fromResult(false);
              }
              return Deferred.fromError(new RuntimeException(
                  String.format("Unable to insert tagset: %s", response.getRowError())));
            } else {
              return Deferred.fromResult(true);
            }
          }
        });
  }

  /**
   * The result of a tagset lookup. If {@link #isFound} returns {@code true},
   * then the tagset was found in the table, and {@link #getID} will return the
   * tagset's ID. If {@link #isFound} returns {@link false}, then the tagset was
   * not found in the table, and {@link #getID} will return the ID that the
   * tagset should be inserted with.
   */
  private static class TagsetLookupResult {
    private final long id;
    private final boolean found;

    private TagsetLookupResult(boolean found, long id) {
      this.found = found;
      this.id = id;
    }

    boolean isFound() {
      return found;
    }

    long getID() {
      return id;
    }

    static TagsetLookupResult found(long id) {
      return new TagsetLookupResult(true, id);
    }

    static TagsetLookupResult notFound(long id) {
      return new TagsetLookupResult(false, id);
    }
  }

  /**
   * Finds a tagset in the {@code tagset} table.
   */
  private final class TagsetLookupCB implements Callback<Deferred<TagsetLookupResult>, RowResultIterator> {
    private final ByteBuffer tagset;
    private AsyncKuduScanner scanner;
    private long id;
    private long probe;

    /**
     * Create a new {@code TagsetLookupCB} looking for a tagset starting with the provided ID.
     * @param tagset the tagset being looked up
     * @param id the ID that the scanner is looking up
     * @param scanner the initialscanner
     */
    TagsetLookupCB(ByteBuffer tagset, long id, AsyncKuduScanner scanner) {
      this.tagset = tagset;
      this.scanner = scanner;
      this.id = id;
      this.probe = id;
    }

    @Override
    public Deferred<TagsetLookupResult> call(RowResultIterator rows) throws Exception {
      LOG.debug("Received tagset lookup results: {}", this);
      for (RowResult row : rows) {
        long rowID = row.getLong(tagsetIDColumnIndex);
        Preconditions.checkState(rowID >= probe);
        if (rowID != probe) {
          // We found a hole in the table where we expected the tagset.
          return Deferred.fromResult(TagsetLookupResult.notFound(probe));
        }

        if (row.getBinary(tagsetColumnIndex).equals(tagset)) {
          return Deferred.fromResult(TagsetLookupResult.found(rowID));
        }

        probe++;
      }

      // We probed through the entire RowResult and didn't find the tagset.
      if (!scanner.hasMoreRows()) {
        if (probe < id + TAGS_PER_SCAN) {
          // We found a hole at the end of the scan.
          return Deferred.fromResult(TagsetLookupResult.notFound(probe));
        }
        // The current scanner has been exhausted; create a new scanner from the
        // latest probe point.
        scanner = tagsetScanner(probe);
        id = probe;
      }
      return scanner.nextRows().addCallbackDeferring(this);
    }

    @Override
    public String toString() {
      Messages.Tagset tags = deserializeTagset(tagset);
      StringBuilder sb = new StringBuilder("TagsetLookupCB { ID: ");
      sb.append(id);
      sb.append(", tags: [");
      List<String> tagStrings = new ArrayList<>();
      for (Messages.Tagset.Tag tag : tags.getTagsList()) {
        tagStrings.add(String.format("%s = %s", tag.getKey(), tag.getValue()));
      }
      Joiner.on(", ").appendTo(sb, tagStrings);
      sb.append("] }");
      return sb.toString();
    }
  }

  private Deferred<Long> insertTags(ByteBuffer tagset, final long id) {
    LOG.debug("Inserting tags. ID: {}, tags: {}", id, deserializeTagset(tagset));
    Messages.Tagset tags = deserializeTagset(tagset);
    AsyncKuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    for (Messages.Tagset.Tag tag : tags.getTagsList()) {
      Insert insert = tagsTable.newInsert();
      // TODO: check with JD if the below fails, it will be caught in the flush
      insert.getRow().addString(tagsKeyColumnIndex, tag.getKey());
      insert.getRow().addString(tagsValueColumnIndex, tag.getValue());
      insert.getRow().addLong(tagsTagsetIDColumnIndex, id);
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
