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

import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.sangupta.murmur.Murmur2;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.List;
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

@InterfaceAudience.Private
@ThreadSafe
public class TagsetCache {
  private final KuduTSSchema schema;
  private final AsyncKuduClient client;
  private final KuduTable tagsetsTable;
  private final ColumnSchema tagsetColumn;
  private final int tagsetIDColumnIndex;
  private final int tagsetColumnIndex;
  private final List<Integer> columnIndexes;

  private static final long SEED = 0X9883143CFDDB5C3FL;

  /** Map of tagset to tagset ID. */
  private final LoadingCache<String, Deferred<Long>> tagsets;

  public TagsetCache(KuduTSSchema schema, AsyncKuduClient client, KuduTable tagsetsTable) {
    this.schema = schema;
    this.client = client;
    this.tagsetsTable = tagsetsTable;
    this.tagsetIDColumnIndex = schema.getTagsetsSchema().getColumnIndex("id");
    this.tagsetColumnIndex = schema.getTagsetsSchema().getColumnIndex("tagset");
    this.columnIndexes = ImmutableList.of(tagsetIDColumnIndex, tagsetColumnIndex);
    this.tagsetColumn = schema.getTagsetsSchema().getColumnByIndex(tagsetColumnIndex);
    this.tagsets = CacheBuilder.newBuilder()
                               .maximumSize(1024 * 1024)
                               .build(new CacheLoader<String, Deferred<Long>>() {
                                 @Override
                                 public Deferred<Long> load(String tagset) throws Exception {
                                   return getTagsetID(tagset);
                                 }
                               });
  }

  /**
   * Reads the ID of a tagset from the {@code tagset} table. If the tagset
   * doesn't exist in the table, then it is added along with corresponding
   * entries in the {@code tags} table.
   *
   * @param tagset the tagset
   * @return the tagset ID
   */
  private Deferred<Long> getTagsetID(final String tagset) {
    byte[] tagsetBuf = tagset.getBytes(Charsets.UTF_8);
    final long hash = Murmur2.hash64(tagsetBuf, tagsetBuf.length, SEED);
    return lookupTagset(tagset, hash).addCallbackDeferring(
        new Callback<Deferred<Long>, TagsetLookupResult>() {
          @Override
          public Deferred<Long> call(TagsetLookupResult result) throws Exception {
            if (result.isFound()) {
              return Deferred.fromResult(result.id);
            } else {
              return insertTagset(tagset, hash).addCallbackDeferring(new InsertTags());
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
    scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetColumn,
                                                                  ComparisonOp.GREATER_EQUAL,
                                                                  from));
    if (from < Long.MAX_VALUE - 10) {
      scanBuilder.addPredicate(KuduPredicate.newComparisonPredicate(tagsetColumn,
                                                                    ComparisonOp.LESS,
                                                                    from + 10));
    }
    scanBuilder.setProjectedColumnIndexes(columnIndexes);
    return scanBuilder.build();
  }

  private Deferred<TagsetLookupResult> lookupTagset(String tagset, long hash) {
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
  private Deferred<OperationResponse> insertTagsetWithID(final String tagset, final long id) {
    final AsyncKuduSession session = client.newSession();
    Insert insert = tagsetsTable.newInsert();
    insert.getRow().addLong(tagsetIDColumnIndex, id);
    insert.getRow().addString(tagsetColumnIndex, tagset);
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
  private Deferred<Long> insertTagset(String tagset, long hash) {
    return insertTagsetWithID(tagset, hash).addCallbackDeferring(new TagsetInsert(tagset, hash));
  }

  private final class Loader extends CacheLoader<String, Deferred<Long>> {
    @Override
    public Deferred<Long> load(String tagset) throws Exception {
      return getTagsetID(tagset);
    }
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
    private final String tagset;
    private AsyncKuduScanner scanner;
    private long probe;

    public TagsetLookup(String tagset, long probeFrom, AsyncKuduScanner scanner) {
      this.tagset = tagset;
      this.scanner = scanner;
      this.probe = probeFrom;
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
        scanner = tagsetScanner(probe);
      }
      return scanner.nextRows().addCallbackDeferring(this);
    }
  }

  private final class TagsetInsert implements Callback<Deferred<Long>, OperationResponse> {
    private final String tagset;
    private long id;

    public TagsetInsert(String tagset, long id) {
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
    @Override
    public Deferred<Long> call(Long id) throws Exception {
      return Deferred.fromResult(id);
    }
  }
}
