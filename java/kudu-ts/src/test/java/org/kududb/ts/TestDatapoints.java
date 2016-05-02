package org.kududb.ts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestDatapoints {

  @Test
  public void testDatapointAccessors() throws Exception {
    int[] tagsetIDs = { 0 };
    long[] timestamps = { 0, 1, 10, 100, 101, 102, 104, 105, 200, 10000 };
    double[] values = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    Datapoints datapoints = new Datapoints("test", tagsetIDs, timestamps, values);

    assertEquals(10, datapoints.size());

    for (int i = 0; i < 10; i++) {
      assertEquals(timestamps[i], datapoints.timestamp(i));
      assertEquals(values[i], datapoints.value(i), 0);
    }
  }

  @Test
  public void testDatapointIteratorSeek() throws Exception {
    int[] tagsetIDs = { 0 };
    long[] timestamps = { 10, 100, 101, 102, 104, 105, 200, 10000 };
    double[] values = { 0, 1, 2, 3, 4, 5, 6, 7 };

    Datapoints datapoints = new Datapoints("test", tagsetIDs, timestamps, values);
    Datapoints.DatapointIterator iter = datapoints.iterator();

    iter.seek(0);
    assertEquals(10, iter.next().getTime());

    iter.seek(9);
    assertEquals(10, iter.next().getTime());

    iter.seek(10);
    assertEquals(10, iter.next().getTime());

    iter.seek(11);
    assertEquals(100, iter.next().getTime());

    iter.seek(100);
    assertEquals(100, iter.next().getTime());

    iter.seek(50);
    assertEquals(100, iter.next().getTime());

    iter.seek(10000);
    assertEquals(10000, iter.next().getTime());

    iter.seek(10001);
    assertFalse(iter.hasNext());
  }
}
