package org.kududb.client;

import java.util.List;

class PartitionSchema {

  private final RangeSchema rangeSchema;
  private final List<HashBucketSchema> hashBucketSchemas;

  /**
   * Creates a new partition schema from the range and hash bucket schemas.
   *
   * @param rangeSchema the range schema.
   * @param hashBucketSchemas the hash bucket schemas.
   */
  PartitionSchema(RangeSchema rangeSchema, List<HashBucketSchema> hashBucketSchemas) {
    this.rangeSchema = rangeSchema;
    this.hashBucketSchemas = hashBucketSchemas;
  }

  /**
   * Returns the encoded partition key of the row.
   * @return a byte array containing the encoded partition key of the row.
   */
  public byte[] encodePartitionKey(PartialRow row) {
    return new KeyEncoder().encodePartitionKey(row, this);
  }

  public RangeSchema getRangeSchema() {
    return rangeSchema;
  }

  public List<HashBucketSchema> getHashBucketSchemas() {
    return hashBucketSchemas;
  }

  public static class RangeSchema {
    private final List<Integer> columns;

    RangeSchema(List<Integer> columns) {
      this.columns = columns;
    }

    public List<Integer> getColumns() {
      return columns;
    }
  }

  public static class HashBucketSchema {

    private final List<Integer> columns;
    private int numBuckets;
    private int seed;

    HashBucketSchema(List<Integer> columns, int numBuckets, int seed) {
      this.columns = columns;
      this.numBuckets = numBuckets;
      this.seed = seed;
    }

    /**
     * Gets the column IDs of the columns in the hash partition.
     * @return the column IDs of the columns in the has partition.
     */
    public List<Integer> getColumns() {
      return columns;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    public int getSeed() {
      return seed;
    }
  }
}
