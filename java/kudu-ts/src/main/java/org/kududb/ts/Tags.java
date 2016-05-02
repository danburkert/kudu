package org.kududb.ts;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduPredicate.ComparisonOp;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code Tagsets} manages inserting and retrieving tags and their associated
 * tagset IDs from the {@code tags} table.
 */
public class Tags {
  private static final Logger LOG = LoggerFactory.getLogger(Tags.class);

  private static final List<Integer> TAGSET_ID_PROJECTION = ImmutableList.of(KuduTSSchema.TAGS_TAGSET_ID_INDEX);

  private final AsyncKuduClient client;
  private final KuduTable table;

  public Tags(AsyncKuduClient client, KuduTable table) {
    this.client = client;
    this.table = table;
  }

  /**
   * Insert a tagset into the {@code tags} table.
   * @param id the tagset ID.
   * @param tagset the tagset.
   * @return The tagset ID.
   */
  public Deferred<Integer> insertTagset(final int id, final Messages.Tagset tagset) {
    class InsertTagsetCB implements Callback<Deferred<Integer>, List<OperationResponse>> {
      @Override
      public Deferred<Integer> call(List<OperationResponse> responses) throws Exception {
        for (OperationResponse response : responses) {
          if (response.hasRowError()) {
            return Deferred.fromError(new RuntimeException(
                String.format("Unable to insert tag: %s", response.getRowError())));
          }
        }
        return Deferred.fromResult(id);
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this)
                      .add("id", id)
                      .add("tags", Tagsets.tagsetToString(tagset))
                      .toString();
      }
    }

    if (tagset.getTagsList().isEmpty()) { return Deferred.fromResult(id); }

    LOG.debug("Inserting tags; tagsetID: {}, tags: {}", id, Tagsets.tagsetToString(tagset));

    AsyncKuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    for (Messages.Tagset.Tag tag : tagset.getTagsList()) {
      Insert insert = table.newInsert();
      // TODO: check with JD that if the inserts below fail, the error will
      // also be returned in the flush call.
      insert.getRow().addString(KuduTSSchema.TAGS_KEY_INDEX, tag.getKey());
      insert.getRow().addString(KuduTSSchema.TAGS_VALUE_INDEX, tag.getValue());
      insert.getRow().addInt(KuduTSSchema.TAGS_TAGSET_ID_INDEX, id);
      session.apply(insert);
    }

    // TODO: Do we need to handle PleaseThrottleException?  Will the number of
    // tags ever be bigger than the session buffer?

    return session.flush().addCallbackDeferring(new InsertTagsetCB());
  }

  /**
   * Retrieves the tagset IDs of all tagsets which contain the specified tag.
   * The tagset IDs are returned in sorted order.
   *
   * @param key the tag key
   * @param value the tag value
   * @return the sorted tagset IDs
   */
  public Deferred<List<Integer>> getTagsetIDsForTag(final String key, final String value) {
    AsyncKuduScanner.AsyncKuduScannerBuilder scan = client.newScannerBuilder(table);
    scan.addPredicate(KuduPredicate.newComparisonPredicate(KuduTSSchema.TAGS_KEY_COLUMN,
                                                           ComparisonOp.EQUAL, key));
    scan.addPredicate(KuduPredicate.newComparisonPredicate(KuduTSSchema.TAGS_VALUE_COLUMN,
                                                           ComparisonOp.EQUAL, value));
    scan.setProjectedColumnIndexes(TAGSET_ID_PROJECTION);
    final AsyncKuduScanner scanner = scan.build();

    class GetTagCB implements Callback<Deferred<List<Integer>>, RowResultIterator> {
      private final List<Integer> tagsetIDs = new ArrayList<>();
      @Override
      public Deferred<List<Integer>> call(RowResultIterator results) throws Exception {
        for (RowResult result : results) {
          tagsetIDs.add(result.getInt(0));
        }
        if (scanner.hasMoreRows()) {
          return scanner.nextRows().addCallbackDeferring(this);
        }
        return Deferred.fromResult(tagsetIDs);
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this).add("key", key).add("value", value).toString();
      }
    }

    return scanner.nextRows().addCallbackDeferring(new GetTagCB());
  }

  private static final Comparator<Integer> INT_COMPARATOR = new Comparator<Integer>() {
    @Override
    public int compare(Integer a, Integer b) {
      return a.compareTo(b);
    }
  };

  public static Deferred<List<Integer>> unionTagsetIDs(List<Deferred<List<Integer>>> tagsetIDs) {
    class UnionTagsetIDsCB implements Callback<List<Integer>, ArrayList<List<Integer>>> {
      @Override
      public List<Integer> call(ArrayList<List<Integer>> tagsetIDs) throws Exception {
        if (tagsetIDs.isEmpty()) { return ImmutableList.of(); }
        if (tagsetIDs.size() == 1) { return tagsetIDs.get(0); }
        List<Integer> ids = new ArrayList<>();
        Integer previous = null;
        for (Integer id : Iterables.mergeSorted(tagsetIDs, INT_COMPARATOR) ){
          if (!id.equals(previous)) {
            previous = id;
            ids.add(id);
          }
        }
        return ids;
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this).toString();
      }
    }

    return Deferred.group(tagsetIDs).addCallback(new UnionTagsetIDsCB());
  }

  public static Deferred<List<Integer>> intersectTagsetIDs(List<Deferred<List<Integer>>> tagsetIDs) {
    class IntersectTagsetIDsCB implements Callback<List<Integer>, ArrayList<List<Integer>>> {
      public List<Integer> call(ArrayList<List<Integer>> tagsetIDs) throws Exception {
        if (tagsetIDs.isEmpty()) { return ImmutableList.of(); }
        if (tagsetIDs.size() == 1) { return tagsetIDs.get(0); }

        Collections.sort(tagsetIDs, new Comparator<List<Integer>>() {
          @Override
          public int compare(List<Integer> a, List<Integer> b) {
            return Integer.compare(b.size(), a.size());
          }
        });

        List<Integer> smallestSet = Lists.reverse(tagsetIDs.remove(tagsetIDs.size() - 1));
        List<Integer> result = Lists.newArrayList();

        for (Integer id : smallestSet) {
          boolean addToResult = true;
          for (List<Integer> set : tagsetIDs) {
            int index = Collections.binarySearch(set, id);
            if (index >= 0) {
              set.subList(index, set.size()).clear();
            } else {
              addToResult = false;
              set.subList(-index, set.size()).clear();
              break;
            }
          }

          if (addToResult) {
            result.add(id);
          }
        }
        Collections.reverse(result);
        return result;
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this).toString();
      }
    }

    return Deferred.group(tagsetIDs).addCallback(new IntersectTagsetIDsCB());
  }
}
