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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Primitives;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Tagsets} manages looking up tagset IDs and tagsets in the
 * {@code tagsets} table. Tagsets and IDs are cached internally, so that
 * subsequent lookups on the same tagset or ID are fast. If a tagset isn't found
 * during a lookup, it is automatically inserted into the {@code tagsets} table,
 * and its tags are inserted into the {@code tags} table.
 *
 * To guarantee that tagset IDs are unique, the {@code tagsets} table is
 * structured as a linear-probe hash table. The tagset is transformed into a
 * canonical byte representation using an internal protobuf format, and the hash
 * of this canonical value is used as the tagset ID. On ID collision, linear
 * probing is used to find a new ID.
 *
 * Internally, {@code Tagsets} keeps an LRU cache of tagsets and IDs so that
 * lookups of frequently used tagsets are fast.
 *
 * Steps for looking up a new tagset:
 *
 *  1) the tagset is converted to a canonical byte string format
 *     (see {@link #serializeTagset}).
 *  2) the internal LRU cache is queried with the byte string, but the lookup fails.
 *  3) a hash of the tagset's byte string is created with the MurmurHash3_32
 *     algorithm (see {@link #hashSerializedTagset}).
 *  4) up to {@link #TAGSETS_PER_SCAN} tagsets are scanned from the {@code tagsets}
 *     table beginning with the computed hash as the ID.
 *  5) the tagsets returned in the scan are checked in ID order. If the tagset
 *     is found, the corresponding ID is returned. If there is an ID missing
 *     in the results, then the tagset is inserted with that ID (go to step 6).
 *     If every ID is present, but the tagset isn't found, then a new scan is
 *     started (step 4), but using the next available ID as the start.
 *  6) the ID from step 5 is used to insert the rowset into the {@code rowsets}
 *     table. If the insert results in a duplicate primary key error, then
 *     another client has concurrently inserted a rowset using the ID. The
 *     concurrently inserted rowset may or may not match the rowset we tried to
 *     insert, so we return to step 4, but using the duplicate ID instead of the
 *     hash of the tagset.
 *  7) After inserting the tagset successfully in step 6, every tag in the
 *     tagset is inserted into the {@code tags} table. No errors are expected
 *     in this step.
 *
 * Tagset IDs are 32bits, which allows for hundreds of millions of tagset IDs without
 * risking excessive hash collisions.
 */
@InterfaceAudience.Private
@ThreadSafe
class Tagsets {
  private static final Logger LOG = LoggerFactory.getLogger(Tagsets.class);

  /** Number of tags to return per tagset scanner. */
  private long TAGSETS_PER_SCAN = 10;

  private final AsyncKuduClient client;
  private final Tags tags;
  private final KuduTable tagsetsTable;
  private final List<Integer> columnIndexes;

  /** Allows tests to hardcode the tagset hash so that collisions can be simulated. */
  private Integer hashForTesting = null;

  /** Map of tagset to tagset ID. */
  private final LoadingCache<ByteBuffer, Deferred<Integer>> tagsets;

  Tagsets(AsyncKuduClient client, Tags tags, KuduTable tagsetsTable) {
    this.client = client;
    this.tagsetsTable = tagsetsTable;
    this.tags = tags;
    this.columnIndexes = ImmutableList.of(KuduTSSchema.TAGSETS_ID_INDEX,
                                          KuduTSSchema.TAGSETS_TAGSET_INDEX);
    this.tagsets = CacheBuilder.newBuilder()
                               .maximumSize(1024 * 1024)
                               .build(new CacheLoader<ByteBuffer, Deferred<Integer>>() {
                                 @Override
                                 public Deferred<Integer> load(ByteBuffer tagset) throws Exception {
                                   final int hash = hashSerializedTagset(tagset);
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
  Deferred<Integer> getTagsetID(SortedMap<String, String> tagset) {
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

  /**
   * Sets a constant hash value for all tagsets. Allows simulating hash
   * collisions in relatively small tables.
   * @param hashForTesting the overflow hash value
   */
  @VisibleForTesting
  void setHashForTesting(int hashForTesting) {
    this.hashForTesting = hashForTesting;
  }

  @VisibleForTesting
  int hashSerializedTagset(ByteBuffer tagset) {
    if (hashForTesting != null) { return hashForTesting; }
    if (!tagset.hasArray()) {
      throw new IllegalArgumentException("Serialized tagset ByteBuffer must have an array");
    }
    return Hashing.murmur3_32()
                  .hashBytes(tagset.array(), tagset.position(), tagset.limit() - tagset.position())
                  .asInt();
  }

  /**
   * Reads the ID of a tagset from the {@code tagset} table. If the tagset
   * doesn't exist in the table, then it is added along with corresponding
   * entries in the {@code tags} table.
   *
   * @param tagset the serialized tagset
   * @return the tagset ID
   */
  private Deferred<Integer> lookupOrInsertTagset(final ByteBuffer tagset, final int id) {
    Callback<Deferred<Integer>, TagsetLookupResult> lookupResultCB = new Callback<Deferred<Integer>, TagsetLookupResult>() {
      @Override
      public Deferred<Integer> call(TagsetLookupResult result) throws Exception {
        if (result.found) {
          return Deferred.fromResult(result.id);
        } else {
          final int probe = result.id;
          return insertTagset(tagset, probe).addCallbackDeferring(new Callback<Deferred<Integer>, Boolean>() {
            @Override
            public Deferred<Integer> call(Boolean success) throws Exception {
              if (success) {
                return tags.insertTagset(probe, deserializeTagset(tagset));
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

  private Deferred<TagsetLookupResult> lookupTagset(ByteBuffer tagset, int id) {
    LOG.debug("Looking up tagset; id: {}, tags: {}", id, tagsetToString(tagset));
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
  private AsyncKuduScanner tagsetScanner(int id) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scanBuilder = client.newScannerBuilder(tagsetsTable);
    scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(KuduTSSchema.TAGSETS_ID_COLUMN,
                                                                  ComparisonOp.GREATER_EQUAL,
                                                                  id));
    if (id < Integer.MAX_VALUE - TAGSETS_PER_SCAN) {
      scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(KuduTSSchema.TAGSETS_ID_COLUMN,
                                                                    ComparisonOp.LESS,
                                                                    id + TAGSETS_PER_SCAN));
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
  private Deferred<Boolean> insertTagset(final ByteBuffer tagset, final int id) {
    LOG.debug("Inserting tagset; id: {}, tags: {}", id, tagsetToString(tagset));
    final AsyncKuduSession session = client.newSession();
    final Insert insert = tagsetsTable.newInsert();
    insert.getRow().addInt(KuduTSSchema.TAGSETS_ID_INDEX, id);
    insert.getRow().addBinary(KuduTSSchema.TAGSETS_TAGSET_INDEX, tagset);
    return Deferred.fromResult(new Object())
        .addCallbackDeferring(new Callback<Deferred<OperationResponse>, Object>() {
          @Override
          public Deferred<OperationResponse> call(Object obj) throws Exception {
            try {
              return session.apply(insert);
            } catch (PleaseThrottleException e) {
              // TODO: do we need to handle this? we only are adding a single insert to the session.
              LOG.warn("Throttling tagset insert; id: {}, error: {}", id, e);
              return e.getDeferred().addCallbackDeferring(this);
            }
          }
        })
        .addCallbackDeferring(new Callback<Deferred<Boolean>, OperationResponse>() {
          @Override
          public Deferred<Boolean> call(OperationResponse response) throws Exception {
            if (response.hasRowError()) {
              if (response.getRowError().getErrorStatus().isAlreadyPresent()) {
                LOG.info("Attempted to insert duplicate tagset; id: {}, tagset: {}",
                         id, tagsetToString(tagset));
                // TODO: Consider adding a backoff with jitter before attempting
                //       the insert again (if the lookup fails).
                return Deferred.fromResult(false);
              }
              return Deferred.fromError(new RuntimeException(
                  String.format("Unable to insert tagset; id: %s, tagset: %s, error: %s",
                                id, tagsetToString(tagset), response.getRowError())));
            } else {
              return Deferred.fromResult(true);
            }
          }
        });
  }

  /**
   * The result of a tagset lookup. If {@link #found} is {@code true},
   * then the tagset was found in the table, and {@link #id} will contain the
   * tagset's ID. If {@link #found} is {@link false}, then the tagset was
   * not found in the table, and {@link #id} will contain the ID that the
   * tagset should be inserted with.
   */
  private static class TagsetLookupResult {
    private final int id;
    private final boolean found;

    private TagsetLookupResult(boolean found, int id) {
      this.found = found;
      this.id = id;
    }
  }

  /**
   * Finds a tagset in the {@code tagset} table.
   */
  private final class TagsetLookupCB implements Callback<Deferred<TagsetLookupResult>,
                                                         RowResultIterator> {
    private final ByteBuffer tagset;
    private AsyncKuduScanner scanner;
    private int id;
    private int probe;

    /**
     * Create a new {@code TagsetLookupCB} looking for a tagset starting with the provided ID.
     * @param tagset the tagset being looked up
     * @param id the ID that the scanner is looking up
     * @param scanner the initialscanner
     */
    TagsetLookupCB(ByteBuffer tagset, int id, AsyncKuduScanner scanner) {
      this.tagset = tagset;
      this.scanner = scanner;
      this.id = id;
      this.probe = id;
    }

    @Override
    public Deferred<TagsetLookupResult> call(RowResultIterator rows) throws Exception {
      LOG.debug("Received tagset lookup results: id: {}, tags: {}", id, tagsetToString(tagset));
      for (RowResult row : rows) {
        int id = row.getInt(KuduTSSchema.TAGSETS_ID_INDEX);
        Preconditions.checkState(id >= probe);
        if (id != probe) {
          // We found a hole in the table where we expected the tagset.
          return Deferred.fromResult(new TagsetLookupResult(false, probe));
        }

        if (row.getBinary(KuduTSSchema.TAGSETS_TAGSET_INDEX).equals(tagset)) {
          return Deferred.fromResult(new TagsetLookupResult(true, id));
        }

        probe++;
      }

      // We probed through the entire RowResult and didn't find the tagset.
      if (!scanner.hasMoreRows()) {
        if (probe <= Ints.saturatedCast((long) id + TAGSETS_PER_SCAN)) {
          // We found a hole at the end of the scan.
          return Deferred.fromResult(new TagsetLookupResult(false, probe));
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
      return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("tags", tagsetToString(tagset))
                    .toString();
    }
  }

  static String tagsetToString(ByteBuffer tagset) {
    return tagsetToString(deserializeTagset(tagset));
  }

  static String tagsetToString(Messages.Tagset tagset) {
    List<Map.Entry<String, String>> tagEntries = new ArrayList<>();
    for (Messages.Tagset.Tag tag : tagset.getTagsList()) {
      tagEntries.add(Maps.immutableEntry(tag.getKey(), tag.getValue()));
    }

    StringBuilder sb = new StringBuilder();
    sb.append('[');

    Joiner.on(", ").withKeyValueSeparator("=").appendTo(sb, tagEntries);
    sb.append(']');
    return sb.toString();
  }
}
