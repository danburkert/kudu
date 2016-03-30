// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cmath>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/util/test_util.h"

using std::numeric_limits;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace client {

using sp::shared_ptr;

class PredicateItest : public KuduTest {

 protected:

  void SetUp() override {
    // Set up the mini cluster
    cluster_.reset(new MiniCluster(env_.get(), MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
    KuduClientBuilder client_builder;
    ASSERT_OK(cluster_->CreateClient(&client_builder, &client_));
  }

  virtual void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
      cluster_.reset();
    }
  }

  // Creates a key/value table schema with an int64 key and value of the
  // specified type.
  KuduSchema CreateSchema(KuduColumnSchema::DataType value_type) {
    KuduSchema schema;
    {
      KuduSchemaBuilder builder;
      builder.AddColumn("key")->NotNull()->Type(KuduColumnSchema::INT64)->PrimaryKey();
      builder.AddColumn("value")->Type(value_type);
      CHECK_OK(builder.Build(&schema));
    }
    return schema;
  }

  // Count the rows in a table which satisfy the specified predicates.
  int CountRows(const shared_ptr<KuduTable>& table,
                const vector<KuduPredicate*>& predicates) {
    KuduScanner scanner(table.get());
    CHECK_OK(scanner.SetTimeoutMillis(5000));
    for (KuduPredicate* predicate : predicates) {
      CHECK_OK(scanner.AddConjunctPredicate(predicate));
    }
    CHECK_OK(scanner.Open());

    int rows = 0;
    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(scanner.NextBatch(&batch));
      rows += batch.NumRows();
    }
    return rows;
  }

  // Returns a vector of ints from -50 (inclusive) to 50 (exclusive), min and max.
  template <typename T>
  vector<T> CreateIntValues() {
    vector<T> values;
    for (int i = -50; i < 50; i++) {
      values.push_back(i);
    }
    values.push_back(numeric_limits<T>::min());
    values.push_back(numeric_limits<T>::max());
    return values;
  }

  // Check integer predicates against the specified table. The table must have
  // key/value rows with values from CreateIntValues.
  template <typename T>
  void CheckIntPredicates(shared_ptr<KuduTable> table) {
    ASSERT_EQ(103, CountRows(table, {}));

    { // value = 0
      ASSERT_EQ(1, CountRows(table, {
            table->NewComparisonPredicate("value", KuduPredicate::EQUAL, KuduValue::FromInt(0)),
      }));
    }

    { // value >= 0
      ASSERT_EQ(51, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(0)),
      }));
    }

    { // value <= 0
      ASSERT_EQ(52, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(0)),
      }));
    }

    { // value >= max
      ASSERT_EQ(1, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::max())),
      }));
    }

    { // value >= -100 value <= 100
      ASSERT_EQ(100, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(-100)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(100)),
      }));
    }

    { // value >= min value <= max
      ASSERT_EQ(102, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::min())),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::max())),
      }));
    }
  }

  // Returns a vector of floating point numbers from -50.50 (inclusive) to 49.49
  // (exclusive) (100 values), plus min, max, two subnormals around 0,
  // positive and negatic infinity, and NaN.
  template <typename T>
  vector<T> CreateFloatingPointValues() {
    vector<T> values;
    for (int i = -50; i < 50; i++) {
      //values.push_back(static_cast<T>(i) + static_cast<T>(i) / 100);
    }
    //values.push_back(numeric_limits<T>::min());
    //values.push_back(numeric_limits<T>::max());
    //values.push_back(-numeric_limits<T>::denorm_min());
    //values.push_back(numeric_limits<T>::denorm_min());
    //values.push_back(-numeric_limits<T>::infinity());
    //values.push_back(numeric_limits<T>::infinity());
    values.push_back(numeric_limits<T>::quiet_NaN());

    return values;
  }

  // Check floating point predicates against the specified table. The table must
  // have key/value rows with values from CreateFloatingPointValues.
  template <typename T>
  void CheckFloatingPointPredicates(shared_ptr<KuduTable> table) {
    ASSERT_EQ(103, CountRows(table, {}));

    { // value = 0
      ASSERT_EQ(1, CountRows(table, {
            table->NewComparisonPredicate("value", KuduPredicate::EQUAL, KuduValue::FromFloat(0)),
      }));
    }

    { // value >= 0
      ASSERT_EQ(51, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(0)),
      }));
    }

    { // value <= 0
      ASSERT_EQ(52, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(0)),
      }));
    }

    { // value >= max
      ASSERT_EQ(1, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::max())),
      }));
    }

    { // value >= -100 value <= 100
      ASSERT_EQ(100, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(-100)),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(100)),
      }));
    }

    { // value >= min value <= max
      ASSERT_EQ(102, CountRows(table, {
            table->NewComparisonPredicate("value",
                                          KuduPredicate::GREATER_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::min())),
            table->NewComparisonPredicate("value",
                                          KuduPredicate::LESS_EQUAL,
                                          KuduValue::FromInt(numeric_limits<T>::max())),
      }));
    }
  }

  shared_ptr<KuduClient> client_;
  gscoped_ptr<MiniCluster> cluster_;
};

TEST_F(PredicateItest, TestBoolPredicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::BOOL);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("bool-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("bool-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (bool b : { false, true }) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetBool("value", b));
      ASSERT_OK(session->Apply(insert.release()));
  }

  // Insert null value
  unique_ptr<KuduInsert> insert(table->NewInsert());
  ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(session->Apply(insert.release()));
  ASSERT_OK(session->Flush());

  ASSERT_EQ(3, CountRows(table, {}));

  { // value = false
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::EQUAL,
                                                        KuduValue::FromBool(false));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }

  { // value = true
    KuduPredicate* pred = table->NewComparisonPredicate("value",
                                                        KuduPredicate::EQUAL,
                                                        KuduValue::FromBool(true));
    ASSERT_EQ(1, CountRows(table, { pred }));
  }
}

TEST_F(PredicateItest, TestInt8Predicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::INT8);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("int8-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("int8-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (int8_t value : CreateIntValues<int8_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt8("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int8_t>(table);
}

TEST_F(PredicateItest, TestInt16Predicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::INT16);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("int16-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("int16-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (int16_t value : CreateIntValues<int16_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt16("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int16_t>(table);
}

TEST_F(PredicateItest, TestInt32Predicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::INT32);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("int32-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("int32-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (int32_t value : CreateIntValues<int32_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt32("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int32_t>(table);
}

TEST_F(PredicateItest, TestInt64Predicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::INT64);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("int64-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("int64-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (int64_t value : CreateIntValues<int64_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetInt64("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int64_t>(table);
}

TEST_F(PredicateItest, TestTimestampPredicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::TIMESTAMP);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("timestamp-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("timestamp-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (int64_t value : CreateIntValues<int64_t>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetTimestamp("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  CheckIntPredicates<int64_t>(table);
}

TEST_F(PredicateItest, TestFloatPredicates) {
  KuduSchema schema = CreateSchema(KuduColumnSchema::FLOAT);
  unique_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name("float-table")
                          .schema(&schema)
                          .num_replicas(1)
                          .Create());

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable("float-table", &table));

  shared_ptr<KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(10000);
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  int i = 0;
  for (float value : CreateFloatingPointValues<float>()) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt64("key", i++));
      ASSERT_OK(insert->mutable_row()->SetFloat("value", value));
      ASSERT_OK(session->Apply(insert.release()));
  }
  unique_ptr<KuduInsert> null_insert(table->NewInsert());
  ASSERT_OK(null_insert->mutable_row()->SetInt64("key", i++));
  ASSERT_OK(null_insert->mutable_row()->SetNull("value"));
  ASSERT_OK(session->Apply(null_insert.release()));
  ASSERT_OK(session->Flush());

  //ASSERT_EQ(108, CountRows(table, {}));

  { // value = 0
    //ASSERT_EQ(1, CountRows(table, {
          //table->NewComparisonPredicate("value", KuduPredicate::EQUAL, KuduValue::FromFloat(0)),
          //}));

    KuduScanner scanner(table.get());
    CHECK_OK(scanner.SetTimeoutMillis(5000));
    CHECK_OK(scanner.AddConjunctPredicate(
        table->NewComparisonPredicate("value", KuduPredicate::GREATER_EQUAL, KuduValue::FromFloat(1000))));
    CHECK_OK(scanner.Open());

    while (scanner.HasMoreRows()) {
      KuduScanBatch batch;
      CHECK_OK(scanner.NextBatch(&batch));
      for (KuduRowResult row : batch) {
        float f;
        CHECK_OK(row.GetFloat("value", &f));
        LOG(INFO) << "got value: " << f;
      }
    }
  }

  //{ // value >= 0
    //ASSERT_EQ(53, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(0)),
          //}));
  //}

  //{ // value <= 0
    //ASSERT_EQ(54, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::LESS_EQUAL,
                                        //KuduValue::FromFloat(0)),
          //}));
  //}

  //{ // value >= max
    //ASSERT_EQ(2, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(FLT_MAX)),
          //}));
  //}

  //{ // value >= +inifinity
    //ASSERT_EQ(2, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(INFINITY)),
          //}));
  //}

  //{ // value >= -100 value <= 100
    //ASSERT_EQ(102, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(-100)),
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::LESS_EQUAL,
                                        //KuduValue::FromFloat(100)),
          //}));
  //}

  //{ // value >= -50 value <= 50
    //ASSERT_EQ(100, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(-50)),
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::LESS_EQUAL,
                                        //KuduValue::FromFloat(50)),
          //}));
  //}

  //{ // value >= min value <= max
    //ASSERT_EQ(104, CountRows(table, {
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::GREATER_EQUAL,
                                        //KuduValue::FromFloat(FLT_MIN)),
          //table->NewComparisonPredicate("value",
                                        //KuduPredicate::LESS_EQUAL,
                                        //KuduValue::FromFloat(FLT_MAX)),
    //}));
  //}
}



} // namespace kudu
} // namespace client
