// Copyright (c) 2013, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

class TestRowChangeList : public KuduTest {
 public:
  TestRowChangeList() :
    schema_(CreateSchema())
  {}

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("col1", STRING));
    CHECK_OK(builder.AddColumn("col2", STRING));
    CHECK_OK(builder.AddColumn("col3", UINT32));
    return(builder.Build());
  }

 protected:
  Schema schema_;
};

TEST_F(TestRowChangeList, TestEncodeDecodeUpdates) {
  faststring buf;
  RowChangeListEncoder rcl(&schema_, &buf);

  // Construct an update with several columns changed
  Slice update1("update1");
  Slice update2("update2");
  uint32 update3 = 12345;

  int c0_id = schema_.column_id(0);
  int c1_id = schema_.column_id(1);
  int c2_id = schema_.column_id(2);

  rcl.AddColumnUpdate(c0_id, &update1);
  rcl.AddColumnUpdate(c1_id, &update2);
  rcl.AddColumnUpdate(c2_id, &update3);

  LOG(INFO) << "Encoded: " << HexDump(buf);

  // Read it back.
  EXPECT_EQ(string("SET col1=update1, col2=update2, col3=12345"),
            RowChangeList(Slice(buf)).ToString(schema_));

  RowChangeListDecoder decoder(&schema_, RowChangeList(buf));
  ASSERT_OK(decoder.Init());
  size_t id;
  const void *val;

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_OK(decoder.DecodeNext(&id, &val));
  ASSERT_EQ(c0_id, id);
  ASSERT_TRUE(update1 == *reinterpret_cast<const Slice *>(val));

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_OK(decoder.DecodeNext(&id, &val));
  ASSERT_EQ(c1_id, id);
  ASSERT_TRUE(update2 == *reinterpret_cast<const Slice *>(val));

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_OK(decoder.DecodeNext(&id, &val));
  ASSERT_EQ(c2_id, id);

  ASSERT_FALSE(decoder.HasNext());
}

TEST_F(TestRowChangeList, TestDeletes) {
  faststring buf;
  RowChangeListEncoder rcl(&schema_, &buf);

  // Construct a deletion.
  rcl.SetToDelete();

  LOG(INFO) << "Encoded: " << HexDump(buf);

  // Read it back.
  EXPECT_EQ(string("DELETE"), RowChangeList(Slice(buf)).ToString(schema_));

  RowChangeListDecoder decoder(&schema_, RowChangeList(buf));
  ASSERT_OK(decoder.Init());
  ASSERT_TRUE(decoder.is_delete());
}

TEST_F(TestRowChangeList, TestReinserts) {
  RowBuilder rb(schema_);
  rb.AddString(Slice("hello"));
  rb.AddString(Slice("world"));
  rb.AddUint32(12345);

  // Construct a REINSERT.
  faststring buf;
  RowChangeListEncoder rcl(&schema_, &buf);
  rcl.SetToReinsert(rb.data());

  LOG(INFO) << "Encoded: " << HexDump(buf);

  // Read it back.
  EXPECT_EQ(string("REINSERT (string col1=hello, string col2=world, uint32 col3=12345)"),
            RowChangeList(Slice(buf)).ToString(schema_));

  RowChangeListDecoder decoder(&schema_, RowChangeList(buf));
  ASSERT_OK(decoder.Init());
  ASSERT_TRUE(decoder.is_reinsert());
  ASSERT_EQ(decoder.reinserted_row_slice(), rb.data());
}

TEST_F(TestRowChangeList, TestInvalid_EmptySlice) {
  RowChangeListDecoder decoder(&schema_, RowChangeList(Slice()));
  ASSERT_STR_CONTAINS(decoder.Init().ToString(),
                      "empty changelist");
}

TEST_F(TestRowChangeList, TestInvalid_BadTypeEnum) {
  RowChangeListDecoder decoder(&schema_, RowChangeList(Slice("\xff", 1)));
  ASSERT_STR_CONTAINS(decoder.Init().ToString(),
                      "Corruption: bad type enum value: 255 in \\xff");
}

TEST_F(TestRowChangeList, TestInvalid_TooLongDelete) {
  RowChangeListDecoder decoder(&schema_, RowChangeList(Slice("\x02""blahblah")));
  ASSERT_STR_CONTAINS(decoder.Init().ToString(),
                      "Corruption: DELETE changelist too long");
}


TEST_F(TestRowChangeList, TestInvalid_TooShortReinsert) {
  RowChangeListDecoder decoder(&schema_, RowChangeList(Slice("\x03")));
  ASSERT_STR_CONTAINS(decoder.Init().ToString(),
                      "Corruption: REINSERT changelist wrong length");
}

} // namespace kudu
