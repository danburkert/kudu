package org.kududb.client;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedBytes;

import org.kududb.Common;

public class Partition implements Comparable<Partition> {
  byte[] partitionKeyStart;
  byte[] partitionKeyEnd;

  byte[] rangeKeyStart;
  byte[] rangeKeyEnd;

  List<Integer> hashBuckets;

  // Size of an encoded hash bucket component in a partition key.
  private static final int ENCODED_BUCKET_SIZE = 4;

  /**
   * Constructs a new {@code Partition} instance from the a protobuf message.
   * @param pb the protobuf message.
   * @return the {@code Partition} corresponding to the message.
   */
  static Partition fromPB(Common.PartitionPB pb) {
    return new Partition(pb.getPartitionKeyStart().toByteArray(),
        pb.getPartitionKeyEnd().toByteArray(),
        pb.getHashBucketsList());

  }

  private Partition(byte[] partitionKeyStart,
                    byte[] partitionKeyEnd,
                    List<Integer> hashBuckets) {
    this.partitionKeyStart = partitionKeyStart;
    this.partitionKeyEnd = partitionKeyEnd;
    this.hashBuckets = hashBuckets;
    this.rangeKeyStart = rangeKey(partitionKeyStart, hashBuckets.size());
    this.rangeKeyEnd = rangeKey(partitionKeyEnd, hashBuckets.size());
  }

  public byte[] getPartitionKeyStart() {
    return partitionKeyStart;
  }

  public byte[] getPartitionKeyEnd() {
    return partitionKeyEnd;
  }

  public byte[] getRangeKeyStart() {
    return rangeKeyStart;
  }

  public byte[] getRangeKeyEnd() {
    return rangeKeyEnd;
  }

  public List<Integer> getHashBuckets() {
    return hashBuckets;
  }

  /**
   * @return true if the partition is the absolute start partition.
   */
  public boolean isStartPartition() {
    return partitionKeyStart == AsyncKuduClient.EMPTY_ARRAY;
  }

  /**
   * @return true if the partition is the absolute end partition.
   */
  public boolean isEndPartition() {
    return partitionKeyEnd == AsyncKuduClient.EMPTY_ARRAY;
  }

  /**
   * Equality only holds for partitions from the same table. Partition equality only takes into
   * account the partition keys, since there is a 1 to 1 correspondence between partition keys and
   * the hash buckets and range keys.
   *
   * @return the hash code.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Partition partition = (Partition) o;
    return Arrays.equals(partitionKeyStart, partition.partitionKeyStart)
        && Arrays.equals(partitionKeyEnd, partition.partitionKeyEnd);
  }

  /**
   * The hash code only takes into account the partition keys, since there is a 1 to 1
   * correspondence between partition keys and the hash buckets and range keys.
   *
   * @return the hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(Arrays.hashCode(partitionKeyStart), Arrays.hashCode(partitionKeyEnd));
  }

  /**
   * Partition comparison is only reasonable when comparing partitions from the same table, and
   * since Kudu does not yet allow partition splitting, no two distinct partitions can have the
   * same start partition key. Accordingly, partitions are compared strictly by the start partition
   * key.
   *
   * @param other the other partition of the same table.
   * @return the comparison of the partitions.
   */
  @Override
  public int compareTo(Partition other) {
    return Bytes.memcmp(this.partitionKeyStart, other.partitionKeyStart);
  }

  /**
   * Returns the range key portion of a partition key given the number of buckets in the partition
   * schema.
   * @param partitionKey the partition key containing the range key.
   * @param numHashBuckets the number of hash bucket components of the table.
   * @return the range key.
   */
  private static byte[] rangeKey(byte[] partitionKey, int numHashBuckets) {
    int bucketsLen= numHashBuckets * ENCODED_BUCKET_SIZE;
    if (partitionKey.length > bucketsLen) {
      return Arrays.copyOfRange(partitionKey, bucketsLen, partitionKey.length - bucketsLen);
    } else {
      return AsyncKuduClient.EMPTY_ARRAY;
    }
  }
}
