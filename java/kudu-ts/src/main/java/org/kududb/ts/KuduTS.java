
package org.kududb.ts;

import com.google.common.base.Objects;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.KuduTable;
import org.kududb.client.MasterErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@ThreadSafe
public class KuduTS implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(KuduTS.class);

  private final AsyncKuduClient client;
  private String name;

  private final Metrics metrics;
  private final Tagsets tagsets;
  private final Tags tags;

  private KuduTS(AsyncKuduClient client,
                 String name,
                 Metrics metrics,
                 Tagsets tagsets,
                 Tags tags) {
    this.client = client;
    this.name = name;
    this.metrics = metrics;
    this.tags = tags;
    this.tagsets = tagsets;
  }

  /**
   * Creates (if necessary) and opens a Kudu TS instance on a Kudu cluster.
   *
   * @param kuduMasterAddressess list of "host:port" pair master addresses
   * @param name the name of the Kudu timeseries store. Multiple instances of
   *             Kudu TS can occupy the same Kudu cluster by using a different name.
   * @return the opened {@code KuduTS}.
   * @throws Exception on error
   */
  public static KuduTS open(List<String> kuduMasterAddressess,
                            String name) throws Exception {
    AsyncKuduClient client = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasterAddressess).build();

    Deferred<KuduTable> metricsDeferred = openOrCreateTable(client, Tables.metricsTableName(name), Tables.METRICS_SCHEMA);
    Deferred<KuduTable> tagsetsDeferred = openOrCreateTable(client, Tables.tagsetsTableName(name), Tables.TAGSETS_SCHEMA);
    Deferred<KuduTable> tagsDeferred = openOrCreateTable(client, Tables.tagsTableName(name), Tables.TAGS_SCHEMA);
    KuduTable metricsTable = metricsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsetsTable = tagsetsDeferred.join(client.getDefaultAdminOperationTimeoutMs());
    KuduTable tagsTable = tagsDeferred.join(client.getDefaultAdminOperationTimeoutMs());

    Tags tags = new Tags(client, tagsTable);
    Tagsets tagsets = new Tagsets(client, tags, tagsetsTable);
    Metrics metrics = new Metrics(client, metricsTable, tagsets);
    return new KuduTS(client, name, metrics, tagsets, tags);
  }

  private static Deferred<KuduTable> openOrCreateTable(final AsyncKuduClient client,
                                                       final String table,
                                                       final Schema schema) throws Exception {
    class CreateTableErrback implements Callback<Deferred<KuduTable>, Exception> {
      @Override
      public Deferred<KuduTable> call(Exception e) throws Exception {
        if (e instanceof MasterErrorException) {
          LOG.debug("Creating table {}", table);
          return client.createTable(table, schema);
        } else {
          throw e;
        }
      }
      @Override
      public String toString() {
        return Objects.toStringHelper(this).add("table", table).toString();
      }
    }

    return client.openTable(table).addErrback(new CreateTableErrback());
  }

  /**
   * Create a new {@link WriteBatch} for inserting datapoints into the timeseries table.
   * @return a new {@code WriteBatch}
   */
  public WriteBatch writeBatch() {
    return new WriteBatch(this);
  }

  /**
   * Query the timeseries table.
   * @param query parameters
   * @return the queried datapoints
   * @throws Exception on error
   */
  public Datapoints query(final Query query) throws Exception {
    class ScanSeriesCB implements Callback<Deferred<ArrayList<Datapoints>>, IntVec> {
      @Override
      public Deferred<ArrayList<Datapoints>> call(IntVec tagsetIDs) throws Exception {
        List<Deferred<Datapoints>> series = new ArrayList<>(tagsetIDs.len());
        IntVec.Iterator iter = tagsetIDs.iterator();
        while (iter.hasNext()) {
          series.add(metrics.scanSeries(query.getMetric(),
                                        iter.next(),
                                        query.getStart(),
                                        query.getEnd(),
                                        query.getDownsampler(),
                                        query.getDownsampleInterval()));
        }
        return Deferred.group(series);
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(this)
                      .add("query", query)
                      .toString();
      }
    }

    List<Datapoints> series = tags.getTagsetIDsForTags(query.getTags())
                                  .addCallbackDeferring(new ScanSeriesCB())
                                  .joinUninterruptibly(client.getDefaultOperationTimeoutMs());

    if (query.getInterpolator() == null) {
      return Datapoints.aggregate(series, query.getAggregator());
    } else {
      return Datapoints.aggregate(series, query.getAggregator(), query.getInterpolator());
    }
  }

  /**
   * Returns the {@link AsyncKuduClient} being used by this {@code KuduTS}.
   * Package private because the client is internal.
   * @return the client instance
   */
  AsyncKuduClient getClient() {
    return client;
  }

  /**
   * Returns the {@link Metrics} being used by this {@code KuduTS}.
   * Package private because the {@code Metrics} is internal.
   * @return the metrics instance
   */
  Metrics getMetrics() {
    return metrics;
  }

  /**
   * Returns the {@link Tags} being used by this {@code KuduTS}.
   * Package private because the {@code Tags} is internal.
   * @return the tags instance
   */
  Tags getTags() {
    return tags;
  }

  /**
   * Returns the {@link Tagsets} being used by this {@code KuduTS}.
   * Package private because the {@code Tagsets} is internal.
   * @return the tagsets instance
   */
  Tagsets getTagsets() {
    return tagsets;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name).toString();
  }
}