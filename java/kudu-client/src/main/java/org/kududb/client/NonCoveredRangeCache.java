package org.kududb.client;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of the non-covered range partitions in a Kudu table.
 */
@ThreadSafe
@InterfaceAudience.Private
final class NonCoveredRangeCache {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);
  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  private final ConcurrentNavigableMap<byte[], byte[]> nonCoveredRanges =
      new ConcurrentSkipListMap<>(COMPARATOR);

  /**
   *
   * @param partitionKey
   * @return
   */
  public Pair<byte[], byte[]> getNonCoveredRange(byte[] partitionKey) {
    Map.Entry<byte[], byte[]> range = nonCoveredRanges.floorEntry(partitionKey);
    if (range == null) return null;
    if (COMPARATOR.compare(partitionKey, range.getKey()) >= 0 &&
        (range.getValue().length == 0 || COMPARATOR.compare(partitionKey, range.getValue()) < 0)) {
      return new Pair<>(range.getKey(), range.getValue());
    } else {
      return null;
    }
  }

  public void addNonCoveredRange(byte[] startPartitionKey, byte[] endPartitionKey) {
    if (startPartitionKey == null || endPartitionKey == null) {
      throw new IllegalArgumentException("Non-covered partition range keys may not be null");
    }
    if (nonCoveredRanges.put(startPartitionKey, endPartitionKey) == null) {
      LOG.info("Discovered non-covered partition range [{}, {})",
               Bytes.hex(startPartitionKey), Bytes.hex(endPartitionKey));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    int i = 1;
    for (Map.Entry<byte[], byte[]> range : nonCoveredRanges.entrySet()) {
      if (i++ < nonCoveredRanges.size()) sb.append(", ");
      sb.append('[');
      sb.append(BaseEncoding.base16().encode(range.getKey()));
      sb.append(',');
      sb.append(' ');
      sb.append(BaseEncoding.base16().encode(range.getValue()));
      sb.append(')');
    }
    sb.append(']');
    return sb.toString();
  }
}
