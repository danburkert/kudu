package org.kududb.ts;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;
import org.kududb.client.BaseKuduTest;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKuduTS extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTS.class);

  private final Random rand = new Random();

  private static void datapointsEqual(Datapoints a, Datapoints b) {
    assertEquals(a.getMetric(), b.getMetric());

    IntVec aTagsetIDs = a.getTagsetIDs().clone();
    aTagsetIDs.sort();
    IntVec bTagsetIDs = a.getTagsetIDs().clone();
    bTagsetIDs.sort();
    assertEquals(aTagsetIDs, bTagsetIDs);


    assertEquals(a.getTimes(), b.getTimes());
    assertEquals(a.getValues().len(), b.getValues().len());

    for (int i = 0; i < a.getValues().len(); i++) {
      assertEquals(a.getValue(i), b.getValue(i), 1e-9);
    }
  }

  private static SortedMap<String, String> tagset(String... tags) {
    if (tags.length % 2 == 1) throw new IllegalArgumentException("tags must have key and value");
    SortedMap<String, String> tagset = new TreeMap();
    for (int i = 0; i < tags.length / 2; i++) {
      tagset.put(tags[i], tags[i + 1]);
    }
    return ImmutableSortedMap.copyOf(tagset);
  }

  /**
   * Generates a random timeseries.
   * @param ts the {@link KuduTS} instance
   * @param metric the timeseries metric
   * @param start the start time in microseconds of the series
   * @param interval the time in microseconds between datapoints
   * @param samples the number of datapoints to generate
   * @param tagset the tags of the timeseries
   * @return the random series
   */
  private Datapoints generateSeries(KuduTS ts,
                                    String metric,
                                    long start,
                                    long interval,
                                    int samples,
                                    SortedMap<String, String> tagset) throws Exception {

    int tagsetID = ts.getTagsets().getTagsetID(tagset).joinUninterruptibly(10000);

    LongVec times = LongVec.withCapacity(samples);
    DoubleVec values = DoubleVec.withCapacity(samples);
    for (int index = 0; index < samples; index++) {
      times.push(start + index * interval);
      values.push(rand.nextDouble() * 10000);
    }
    return new Datapoints(metric, IntVec.wrap(new int[] { tagsetID }), times, values);
  }

  @Test
  public void testCreateAndOpen() throws Exception {
    try (KuduTS tsdb = KuduTS.open(ImmutableList.of(getMasterAddresses()), "testCreateAndOpen")) {}
    try (KuduTS tsdb = KuduTS.open(ImmutableList.of(getMasterAddresses()), "testCreateAndOpen")) {}
  }

  /** Tests writing and querying a single series with no downsampling. */
  @Test
  public void testSingleSeries() throws Exception {
    try (KuduTS ts = KuduTS.open(ImmutableList.of(getMasterAddresses()), "testSingleSeries")) {
      WriteBatch batch = ts.writeBatch();
      batch.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      try {
        String metric = "m";
        SortedMap<String, String> tagset = tagset("k1", "v1");
        Datapoints series = generateSeries(ts, metric, 420000, 10000, 100, tagset);

        for (Datapoint datapoint : series) {
          batch.writeDatapoint(metric, tagset, datapoint.getTime(), datapoint.getValue());
        }
        batch.flush();

        Query query = Query.create(metric, tagset, Aggregators.max());
        assertEquals(series, ts.query(query));
      } finally {
        batch.close();
      }
    }
  }

  /**
   * Tests writing and querying multiple series with no downsampling.
   * The series are overlapping in time, and share a common tag.
   */
  @Test
  public void testMultipleSeries() throws Exception {
    try (KuduTS ts = KuduTS.open(ImmutableList.of(getMasterAddresses()), "testMultipleSeries")) {
      WriteBatch batch = ts.writeBatch();
      batch.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      try {
        String metric = "m";

        List<Datapoints> datapoints = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          SortedMap<String, String> tagset = tagset("k1", "v1",
                                                    "k2", Integer.toString(i),
                                                    "k3", Integer.toString(i % 5));
          Datapoints series = generateSeries(ts, metric, 420000 + (i * 30000), 10000, 100, tagset);
          for (Datapoint datapoint : series) {
            batch.writeDatapoint(metric, tagset, datapoint.getTime(), datapoint.getValue());
          }
          datapoints.add(series);
        }
        batch.flush();

        { // all series
          Query query = Query.create(metric, tagset("k1", "v1"), Aggregators.mean());
          datapointsEqual(Datapoints.aggregate(datapoints, Aggregators.mean()),
                          ts.query(query));
        }
        { // single series
          Query query = Query.create(metric, tagset("k2", "5"), Aggregators.mean());
          datapointsEqual(datapoints.get(6),
                          ts.query(query));
        }




      } finally {
        batch.close();
      }
    }
  }

  /**
   * Tests writing and querying multiple series with no downsampling.
   * The series are overlapping in time, and share a common tag.
   */
  @Test
  public void testSingleSeriesDownsample() throws Exception {
    try (KuduTS ts = KuduTS.open(ImmutableList.of(getMasterAddresses()), "testSingleSeries")) {
      WriteBatch batch = ts.writeBatch();
      batch.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      try {
        String metric = "m";
        SortedMap<String, String> tagset = tagset("k1", "v1");
        Datapoints series = generateSeries(ts, metric, 420000, 10000, 100, tagset);

        for (Datapoint datapoint : series) {
          batch.writeDatapoint(metric, tagset, datapoint.getTime(), datapoint.getValue());
        }
        batch.flush();

        Query query = Query.create(metric, tagset, Aggregators.max())
                           .setDownsampler(Aggregators.mean(), 20000);



      } finally {
        batch.close();
      }
    }

  }
}
