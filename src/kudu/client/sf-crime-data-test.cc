#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <fstream>
#include <streambuf>


#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/geometry/s2latlng.h"
#include "kudu/gutil/geometry/s2cellid.h"
#include "kudu/util/env.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduScanner;
using kudu::client::KuduRowResult;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using std::getline;
using std::ifstream;
using std::string;
using std::vector;

namespace kudu {

static vector<vector<string>> parse_dataset() {
  vector<vector<string>> data;

  ifstream file("/data/sf-crime-data-2014.csv");

  string line;
  while (getline(file, line)) {
    vector<string> fields;
    SplitCSVLineWithDelimiterForStrings(line, ',', &fields);
    data.emplace_back(std::move(fields));
  }


  return data;
}


KuduSchema create_schema() {
  // HEADER: IncidntNum,Category,Descript,DayOfWeek,Date,Time,PdDistrict,Resolution,Address,X,Y,Location,PdId
  KuduSchema s;
  KuduSchemaBuilder b;
  b.AddColumn("location")->Type(KuduColumnSchema::S2CELL)->NotNull();
  b.AddColumn("pdid")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("incident_number")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("category")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("description")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("day_of_week")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("date")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("time")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("police_district")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("resolution")->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn("address")->Type(KuduColumnSchema::STRING)->NotNull();
  b.SetPrimaryKey({ "location", "pdid" });
  CHECK_OK(b.Build(&s));
  return s;
}

static shared_ptr<KuduClient> get_client() {
  shared_ptr<KuduClient> client;
  CHECK_OK(KuduClientBuilder().add_master_server_addr("127.0.0.1").Build(&client));
  return client;
}

TEST(SfCrimeDataTest, CreateTable) {
  shared_ptr<client::KuduClient> client = get_client();
  KuduTableCreator* creator = client->NewTableCreator();
  KuduSchema schema = create_schema();
  creator->schema(&schema);
  creator->table_name("sf_crime");
  CHECK_OK(creator->Create());
}

TEST(SfCrimeDataTest, DropTable) {
  shared_ptr<client::KuduClient> client = get_client();
  CHECK_OK(client->DeleteTable("sf_crime"));
}

// Crime data comes from
// https://data.sfgov.org/api/views/v2gf-jivt/rows.csv?accessType=DOWNLOAD
// with the header stripped.
TEST(SfCrimeDataTest, LoadData) {
  shared_ptr<KuduClient> client = get_client();
  vector<vector<string>> data = parse_dataset();
  shared_ptr<KuduTable> table;
  client->OpenTable("sf_crime", &table);
  shared_ptr<KuduSession> session = client->NewSession();

  session->SetTimeoutMillis(60000);
  CHECK_OK(session->SetFlushMode(KuduSession::FlushMode::MANUAL_FLUSH));

  for (const vector<string>& record : data) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();

    // HEADER: IncidntNum,Category,Descript,DayOfWeek,Date,Time,PdDistrict,Resolution,Address,X,Y,Location,PdId

    double lat = stod(record[10]);
    double lng = stod(record[9]);
    S2LatLng latlng = S2LatLng::FromDegrees(lat, lng);
    CHECK_OK(row->SetS2Cell("location", S2CellId::FromLatLng(latlng)));

    CHECK_OK(row->SetString("pdid", record[12]));

    CHECK_OK(row->SetString("incident_number", record[0]));
    CHECK_OK(row->SetString("category", record[1]));
    CHECK_OK(row->SetString("description", record[2]));
    CHECK_OK(row->SetString("day_of_week", record[3]));
    CHECK_OK(row->SetString("date", record[4]));
    CHECK_OK(row->SetString("time", record[5]));
    CHECK_OK(row->SetString("police_district", record[6]));
    CHECK_OK(row->SetString("resolution", record[7]));
    CHECK_OK(row->SetString("address", record[8]));

    CHECK_OK(session->Apply(insert));

    if (session->CountBufferedOperations() > 1000) {
      if (!session->Flush().ok()) {
        LOG(WARNING) << "Flush Failed:";
        vector<KuduError *> errors;
        bool overflow;
        session->GetPendingErrors(&errors, &overflow);
        for (auto error : errors) {
          LOG(WARNING) << error->status().ToString();
        }
      }
      CHECK_OK(session->Flush());
    }
  }
  CHECK_OK(session->Flush());
}

static void print_scan_results(KuduScanner& scanner) {
  while (scanner.HasMoreRows()) {
    vector<KuduRowResult> rows;
    scanner.NextBatch(&rows);
    for (const KuduRowResult& row : rows) {
      S2CellId cell;
      CHECK_OK(row.GetS2Cell("location", &cell));
      cout << cell.ToLatLng().ToStringInDegrees() << std::endl;
    }
  }
}

TEST(SfCrimeDataTest, SelectAll) {
  shared_ptr<KuduClient> client = get_client();
  shared_ptr<KuduTable> table;
  client->OpenTable("sf_crime", &table);

  KuduScanner scanner(table.get());
  scanner.Open();
  print_scan_results(scanner);
}

TEST(SfCrimeDataTest, SelectSome) {

  S2LatLng lat_lng = S2LatLng::FromDegrees(37.79,-122.42);
  S2CellId cell = S2CellId::FromLatLng(lat_lng)
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent()
    .parent();

  shared_ptr<KuduClient> client = get_client();
  shared_ptr<KuduTable> table;
  client->OpenTable("sf_crime", &table);

  KuduScanner scanner(table.get());
  CHECK_OK(scanner.AddConjunctPredicate(table->NewS2Predicate("location", cell)));
  scanner.Open();
  print_scan_results(scanner);
}
}
