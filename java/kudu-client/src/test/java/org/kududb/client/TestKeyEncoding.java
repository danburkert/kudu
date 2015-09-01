// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKeyEncoding {

  /**
   * Builds the default partition schema for a schema.
   * @param schema the schema.
   * @return a default partition schema.
   */
  private PartitionSchema defaultPartitionSchema(Schema schema) {
    List<Integer> columnIds = new ArrayList<>();
    for (int i = 0; i < schema.getPrimaryKeyColumnCount(); i++) {
      columnIds.add(i);
    }
    return new PartitionSchema(
        new PartitionSchema.RangeSchema(columnIds),
        ImmutableList.<PartitionSchema.HashBucketSchema>of());
  }

  @Test
  public void testBoolPartitionKeys() {
    List<ColumnSchema> cols1 = new ArrayList<ColumnSchema>();
    cols1.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.BOOL)
        .key(true)
        .build());
    Schema schema = new Schema(cols1);
    PartitionSchema partitionSchema = defaultPartitionSchema(schema);
    KuduTable table = new KuduTable(null, "one", schema, partitionSchema);
    Insert oneKeyInsert = new Insert(table);
    PartialRow row = oneKeyInsert.getRow();
    row.addBoolean("key", true);
    assertTrue(Bytes.pretty(oneKeyInsert.partitionKey()) + " isn't foo",
        Bytes.equals(new byte[]{1}, oneKeyInsert.partitionKey()));

    oneKeyInsert = new Insert(table);
    row = oneKeyInsert.getRow();
    row.addBoolean("key", false);
    assertTrue(Bytes.pretty(oneKeyInsert.partitionKey()) + " isn't foo",
            Bytes.equals(new byte[] {0}, oneKeyInsert.partitionKey()));
  }

  @Test
  public void testPartitionKeys() {
    List<ColumnSchema> cols1 = new ArrayList<ColumnSchema>();
    cols1.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
        .key(true)
        .build());
    Schema schemaOneString = new Schema(cols1);
    PartitionSchema partitionSchemaOneString = defaultPartitionSchema(schemaOneString);
    KuduTable table = new KuduTable(null, "one", schemaOneString, partitionSchemaOneString);
    Insert oneKeyInsert = new Insert(table);
    PartialRow row = oneKeyInsert.getRow();
    row.addString("key", "foo");
    assertTrue(Bytes.pretty(oneKeyInsert.partitionKey()) + " isn't foo", Bytes.equals(new byte[] {'f', 'o',
        'o'}, oneKeyInsert.partitionKey()));

    List<ColumnSchema> cols2 = new ArrayList<ColumnSchema>();
    cols2.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
        .key(true)
        .build());
    cols2.add(new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING)
        .key(true)
        .build());
    Schema schemaTwoString = new Schema(cols2);
    PartitionSchema partitionSchemaTwoString = defaultPartitionSchema(schemaTwoString);
    KuduTable table2 = new KuduTable(null, "two", schemaTwoString, partitionSchemaTwoString);
    Insert twoKeyInsert = new Insert(table2);
    row = twoKeyInsert.getRow();
    row.addString("key", "foo");
    row.addString("key2", "bar");
    assertTrue(Bytes.pretty(twoKeyInsert.partitionKey()) + " isn't foo0x000x00bar",
        Bytes.equals(new byte[] {'f',
        'o', 'o', 0x00, 0x00, 'b', 'a', 'r'}, twoKeyInsert.partitionKey()));

    Insert twoKeyInsertWithNull = new Insert(table2);
    row = twoKeyInsertWithNull.getRow();
    row.addString("key", "xxx\0yyy");
    row.addString("key2", "bar");
    assertTrue(Bytes.pretty(twoKeyInsertWithNull.partitionKey()) + " isn't " +
        "xxx0x000x01yyy0x000x00bar",
        Bytes.equals(new byte[] {'x', 'x', 'x', 0x00, 0x01, 'y', 'y', 'y', 0x00, 0x00, 'b', 'a',
            'r'},
            twoKeyInsertWithNull.partitionKey()));

    // test that we get the correct memcmp result, the bytes are in big-endian order in a key
    List<ColumnSchema> cols3 = new ArrayList<>();
    cols3.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
        .key(true)
        .build());
    cols3.add(new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING)
        .key(true)
        .build());
    Schema schemaIntString = new Schema(cols3);
    PartitionSchema partitionSchemaIntString = defaultPartitionSchema(schemaIntString);
    KuduTable table3 = new KuduTable(null, "three", schemaIntString, partitionSchemaIntString);
    Insert small = new Insert(table3);
    row = small.getRow();
    row.addInt("key", 20);
    row.addString("key2", "data");
    assertEquals(0, Bytes.memcmp(small.partitionKey(), small.partitionKey()));

    Insert big = new Insert(table3);
    row = big.getRow();
    row.addInt("key", 10000);
    row.addString("key2", "data");
    assertTrue(Bytes.memcmp(small.partitionKey(), big.partitionKey()) < 0);
    assertTrue(Bytes.memcmp(big.partitionKey(), small.partitionKey()) > 0);

    // The following tests test our assumptions on unsigned data types sorting from KeyEncoder
    byte four = 4;
    byte onHundredTwentyFour = -4;
    four = Bytes.xorLeftMostBit(four);
    onHundredTwentyFour = Bytes.xorLeftMostBit(onHundredTwentyFour);
    assertTrue(four < onHundredTwentyFour);

    byte[] threeHundred = Bytes.fromInt(300);
    byte[] reallyBigNumber = Bytes.fromInt(-300);
    threeHundred[0] = Bytes.xorLeftMostBit(threeHundred[0]);
    reallyBigNumber[3] = Bytes.xorLeftMostBit(reallyBigNumber[3]);
    assertTrue(Bytes.memcmp(threeHundred, reallyBigNumber) < 0);
  }
}
