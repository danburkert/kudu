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

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/hms/hive_metastore_types.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/hms/hms_client.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DECLARE_bool(force);
DECLARE_int64(timeout_ms);
DECLARE_string(hive_metastore_uris);

DEFINE_bool(dryrun, false,
    "Print a message for each fix, but do not make modifications to Kudu or the Hive Metastore.");
DEFINE_bool(drop_orphan_hms_tables, false,
    "Drop orphan Hive Metastore tables which refer to non-existent Kudu tables.");
DEFINE_bool(create_missing_hms_tables, true,
    "Create a Hive Metastore table for each Kudu table which is missing one.");
DEFINE_bool(fix_inconsistent_tables, true,
    "Fix tables whose Kudu and Hive Metastore metadata differ. If the table name is "
    "different, the table is renamed in Kudu to match the HMS. If the columns "
    "or other metadata is different the HMS is updated to match Kudu.");
DEFINE_bool(upgrade_hms_tables, true,
    "Upgrade Hive Metastore tables from the legacy Impala metadata format to the "
    "new Kudu metadata format.");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduTable;
using client::KuduTableAlterer;
using client::sp::shared_ptr;
using hms::HmsCatalog;
using hms::HmsClient;
using std::cout;
using std::endl;
using std::make_pair;
using std::ostream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Split;
using strings::Substitute;

// Only alter the table in Kudu but not in the Hive Metastore.
Status RenameTableInKuduCatalog(KuduClient* kudu_client,
                                const string& name,
                                const string& new_name) {
  unique_ptr<KuduTableAlterer> alterer(kudu_client->NewTableAlterer(name));
  SetAlterExternalCatalogs(alterer.get(), false);
  return alterer->RenameTo(new_name)
                ->Alter();
}

Status Init(const RunnerContext& context,
            shared_ptr<KuduClient>* kudu_client,
            unique_ptr<HmsCatalog>* hms_catalog) {
  const string& master_addresses = FindOrDie(context.required_args, kMasterAddressesArg);

  if (!hms::HmsCatalog::IsEnabled()) {
    return Status::IllegalState("HMS URIs cannot be empty!");
  }

  // Create Hms Catalog.
  hms_catalog->reset(new hms::HmsCatalog(master_addresses));
  RETURN_NOT_OK((*hms_catalog)->Start());

  // Create a Kudu Client.
  return KuduClientBuilder()
      .default_rpc_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms))
      .master_server_addrs(Split(master_addresses, ","))
      .Build(kudu_client);
}

// TODO(dan): check that the HMS integration isn't enabled before running it.
Status HmsDowngrade(const RunnerContext& context) {
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  Init(context, &kudu_client, &hms_catalog);

  // 1. Identify all Kudu tables in the HMS entries.
  vector<hive::Table> hms_tables;
  RETURN_NOT_OK(hms_catalog->GetKuduTables(&hms_tables));

  // 2. Downgrades all Kudu tables to legacy table format.
  for (auto& hms_table : hms_tables) {
    if (hms_table.parameters[HmsClient::kStorageHandlerKey] == HmsClient::kKuduStorageHandler) {
      RETURN_NOT_OK(hms_catalog->DowngradeToLegacyImpalaTable(
          Substitute("$0.$1", hms_table.dbName, hms_table.tableName)));
    }
  }

  return Status::OK();
}

// Given a kudu table and a hms table, checks if their metadata is in sync.
bool IsSynced(const string& master_addresses,
              const KuduTable& kudu_table,
              const hive::Table& hms_table) {
  Schema schema(client::SchemaFromKuduSchema(kudu_table.schema()));
  hive::Table hms_table_copy(hms_table);
  Status s = HmsCatalog::PopulateTable(kudu_table.id(), kudu_table.name(),
                                       schema, master_addresses, &hms_table_copy);
  return s.ok() && hms_table_copy == hms_table;
}

// Prints catalog information about Kudu tables in data table format to 'out'.
Status PrintKuduTables(const string& master_addrs,
                       const vector<shared_ptr<KuduTable>>& kudu_tables,
                       ostream& out) {
  DataTable table({
      "Kudu table",
      "Kudu table ID",
      "Kudu master addresses",
  });
  for (const auto& kudu_table : kudu_tables) {
    table.AddRow({
        kudu_table->name(),
        kudu_table->id(),
        master_addrs,
    });
  }
  return table.PrintTo(out);
}

// Prints catalog information about HMS tables in data table format to 'out'.
Status PrintHmsTables(vector<hive::Table>* hms_tables, ostream& out) {
  DataTable table({
      "HMS database",
      "HMS table",
      Substitute("HMS $0", HmsClient::kStorageHandlerKey),
      Substitute("HMS $0", HmsClient::kLegacyKuduTableNameKey),
      Substitute("HMS $0", HmsClient::kKuduTableIdKey),
      Substitute("HMS $0", HmsClient::kKuduMasterAddrsKey),
  });
  for (auto& hms_table : *hms_tables) {
    table.AddRow({
        hms_table.dbName,
        hms_table.tableName,
        hms_table.parameters[HmsClient::kStorageHandlerKey],
        hms_table.parameters[HmsClient::kLegacyKuduTableNameKey],
        hms_table.parameters[HmsClient::kKuduTableIdKey],
        hms_table.parameters[HmsClient::kKuduMasterAddrsKey],
    });
  }
  return table.PrintTo(out);
}

// Prints catalog information about Kudu and HMS tables in data table format to 'out'.
Status PrintKuduAndHmsTables(const string& master_addrs,
                             vector<pair<shared_ptr<KuduTable>, hive::Table>>* table_pairs,
                             ostream& out) {
  DataTable table({
      "Kudu table",
      "Kudu table ID",
      "Kudu master addresses",
      "HMS database",
      "HMS table",
      Substitute("HMS $0", HmsClient::kStorageHandlerKey),
      Substitute("HMS $0", HmsClient::kLegacyKuduTableNameKey),
      Substitute("HMS $0", HmsClient::kKuduTableIdKey),
      Substitute("HMS $0", HmsClient::kKuduMasterAddrsKey),
  });
  for (auto& pair : *table_pairs) {
    const KuduTable& kudu_table = *pair.first;
    hive::Table& hms_table = pair.second;
    table.AddRow({
        kudu_table.name(),
        kudu_table.id(),
        master_addrs,
        hms_table.dbName,
        hms_table.tableName,
        hms_table.parameters[HmsClient::kStorageHandlerKey],
        hms_table.parameters[HmsClient::kLegacyKuduTableNameKey],
        hms_table.parameters[HmsClient::kKuduTableIdKey],
        hms_table.parameters[HmsClient::kKuduMasterAddrsKey],
    });
  }

  return table.PrintTo(out);
}

// A report of inconsistent tables in Kudu and the HMS catalogs.
struct CatalogReport {
  // Kudu tables in the HMS catalog which have no corresponding table in the
  // Kudu catalog (including legacy tables).
  vector<hive::Table> orphan_hms_tables;

  // Tables in the Kudu catalog which have no corresponding table in the HMS catalog.
  vector<shared_ptr<KuduTable>> missing_hms_tables;

  // Tables in the Kudu catalog which have a Hive-incompatible name, and which
  // are not referenced by an existing legacy Hive table (otherwise they would
  // fall in to 'inconsistent_tables').
  //
  // These tables can not be automatically corrected by the fix tool.
  vector<shared_ptr<KuduTable>> invalid_name_tables;

  // Legacy Imapala/Kudu tables (storage handler is com.cloudera.kudu.hive.KuduStorageHandler).
  vector<pair<shared_ptr<KuduTable>, hive::Table>> legacy_hms_tables;

  // Kudu tables with multiple HMS table entries. The entries may or may not be legacy.
  //
  // These tables can not be automatically corrected by the fix tool.
  vector<pair<shared_ptr<KuduTable>, hive::Table>> duplicate_hms_tables;

  // Tables whose Kudu catalog table and HMS table are inconsistent.
  vector<pair<shared_ptr<KuduTable>, hive::Table>> inconsistent_tables;
};

// Retrieves the entire Kudu catalog, as well as all Kudu tables in the HMS
// catalog, and compares them to find inconsistencies.
//
// Inconsistencies are bucketed into different groups, corresponding to how they
// can be repaired.
Status AnalyzeCatalogs(const string& master_addrs,
                       HmsCatalog* hms_catalog,
                       KuduClient* kudu_client,
                       CatalogReport* report) {
  // Step 1: retrieve all Kudu tables, and aggregate them by ID and by name. The
  // by-ID map will be used to match the HMS Kudu table entries. The by-name map
  // will be used to match against legacy Impala/Kudu HMS table entries.
  unordered_map<string, shared_ptr<KuduTable>> kudu_tables_by_id;
  unordered_map<string, shared_ptr<KuduTable>> kudu_tables_by_name;
  {
    vector<string> kudu_table_names;
    RETURN_NOT_OK(kudu_client->ListTables(&kudu_table_names));
    for (const string& kudu_table_name : kudu_table_names) {
      shared_ptr<KuduTable> kudu_table;
      // TODO(dan): When the error is NotFound, prepend an admonishment about not
      // running this tool when the catalog is in-flux.
      RETURN_NOT_OK(kudu_client->OpenTable(kudu_table_name, &kudu_table));
      kudu_tables_by_id.emplace(kudu_table->id(), kudu_table);
      kudu_tables_by_name.emplace(kudu_table->name(), std::move(kudu_table));
    }
  }

  // Step 2: retrieve all Kudu table entries in the HMS, filter all orphaned
  // entries which reference non-existent Kudu tables, and group the rest by
  // table ID.
  vector<hive::Table> orphan_tables;
  unordered_map<string, vector<hive::Table>> hms_tables_by_id;
  {
    vector<hive::Table> hms_tables;
    RETURN_NOT_OK(hms_catalog->GetKuduTables(&hms_tables));
    for (hive::Table& hms_table : hms_tables) {
      const string& storage_handler = hms_table.parameters[HmsClient::kStorageHandlerKey];
      if (storage_handler == HmsClient::kKuduStorageHandler) {
        const string& hms_table_id = hms_table.parameters[HmsClient::kKuduTableIdKey];
        shared_ptr<KuduTable>* kudu_table = FindOrNull(kudu_tables_by_id, hms_table_id);
        if (kudu_table) {
          hms_tables_by_id[(*kudu_table)->id()].emplace_back(std::move(hms_table));
        } else {
          orphan_tables.emplace_back(std::move(hms_table));
        }
      } else if (storage_handler == HmsClient::kLegacyKuduStorageHandler) {
        const string& hms_table_name = hms_table.parameters[HmsClient::kLegacyKuduTableNameKey];
        shared_ptr<KuduTable>* kudu_table = FindOrNull(kudu_tables_by_name, hms_table_name);
        if (kudu_table) {
          hms_tables_by_id[(*kudu_table)->id()].emplace_back(std::move(hms_table));
        } else {
          orphan_tables.emplace_back(std::move(hms_table));
        }
      }
    }
  }

  // Step 3: Determine the state of each Kudu table's HMS entry(ies), and bin
  // them appropriately.
  vector<pair<shared_ptr<KuduTable>, hive::Table>> legacy_tables;
  vector<pair<shared_ptr<KuduTable>, hive::Table>> duplicate_tables;
  vector<pair<shared_ptr<KuduTable>, hive::Table>> stale_tables;
  vector<shared_ptr<KuduTable>> missing_tables;
  vector<shared_ptr<KuduTable>> invalid_name_tables;
  for (auto& kudu_table_pair : kudu_tables_by_id) {
    shared_ptr<KuduTable> kudu_table = kudu_table_pair.second;
    vector<hive::Table>* hms_tables = FindOrNull(hms_tables_by_id, kudu_table_pair.first);

    if (!hms_tables) {
      const string& table_name = kudu_table->name();
      string normalized_table_name(table_name.data(), table_name.size());
      Status s = hms::HmsCatalog::NormalizeTableName(&normalized_table_name);
      if (!s.ok()) {
        invalid_name_tables.emplace_back(std::move(kudu_table));
      } else {
        missing_tables.emplace_back(std::move(kudu_table));
      }
    } else if (hms_tables->size() == 1) {
      hive::Table& hms_table = (*hms_tables)[0];
      const string& storage_handler = hms_table.parameters[HmsClient::kStorageHandlerKey];
      if (storage_handler == HmsClient::kKuduStorageHandler &&
          !IsSynced(master_addrs, *kudu_table, hms_table)) {
        stale_tables.emplace_back(make_pair(std::move(kudu_table), std::move(hms_table)));
      } else if (storage_handler == HmsClient::kLegacyKuduStorageHandler) {
        legacy_tables.emplace_back(make_pair(std::move(kudu_table), std::move(hms_table)));
      }
    } else {
      for (hive::Table& hms_table : *hms_tables) {
        duplicate_tables.emplace_back(make_pair(kudu_table, std::move(hms_table)));
      }
    }
  }

  report->orphan_hms_tables.swap(orphan_tables);
  report->missing_hms_tables.swap(missing_tables);
  report->invalid_name_tables.swap(invalid_name_tables);
  report->legacy_hms_tables.swap(legacy_tables);
  report->duplicate_hms_tables.swap(duplicate_tables);
  report->inconsistent_tables.swap(stale_tables);
  return Status::OK();
}

Status CheckHmsMetadata(const RunnerContext& context) {
  const string& master_addrs = FindOrDie(context.required_args, kMasterAddressesArg);
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  RETURN_NOT_OK(Init(context, &kudu_client, &hms_catalog));

  CatalogReport report;
  RETURN_NOT_OK(AnalyzeCatalogs(master_addrs, hms_catalog.get(), kudu_client.get(), &report));

  // TODO: add header saying manual fix

  if (!report.invalid_name_tables.empty()) {
    cout << "Found Kudu table(s) with Hive-incompatible names:" << endl;
    RETURN_NOT_OK(PrintKuduTables(master_addrs, report.invalid_name_tables, cout));
    cout << endl
         << "Suggestion: rename the Kudu table(s) to be Hive-compatible:" << endl;
    for (const auto& table : report.invalid_name_tables) {
      cout << "\t$ kudu table rename_table --alter_external_catalogs=false "
           << master_addrs << " " << table->name() << " <database-name>.<table-name>" << endl;
    }
    cout << endl;
  }

  if (!report.duplicate_hms_tables.empty()) {
    cout << "Found Kudu table(s) with multiple corresponding Hive Metastore tables:" << endl;
    RETURN_NOT_OK(PrintKuduAndHmsTables(master_addrs, &report.duplicate_hms_tables, cout));
    cout << endl
         << "Suggestion: using Impala or the Hive shell, drop the duplicate Hive Metastore tables,"
         << endl
         << "            and consider recreating them as views referencing the base Kudu table."
         << endl
         << endl;
  }

  // TODO: add header saying auto fixable

  if (!report.orphan_hms_tables.empty()) {
    cout << "Found Hive Metastore table(s) which reference a non-existent Kudu table:" << endl;
    RETURN_NOT_OK(PrintHmsTables(&report.orphan_hms_tables, cout));
    cout << endl
         << "Suggestion: use the fix tool to drop the orphan HMS table(s):" << endl
         << "\t$ kudu hms fix --drop_orphan_hms_tables " << master_addrs << endl
         << endl;
  }

  if (!report.missing_hms_tables.empty()) {
    cout << "Found Kudu table(s) without a corresponding Hive Metastore table:" << endl;
    RETURN_NOT_OK(PrintKuduTables(master_addrs, report.missing_hms_tables, cout));
    cout << endl
         << "Suggestion: use the fix tool to create the missing Hive Metastore tables:" << endl
         << "\t$ kudu hms fix " << master_addrs << endl
         << endl;
  }

  if (!report.legacy_hms_tables.empty()) {
    cout << "Found table(s) in the Hive Metastore with the legacy Impala/Kudu metadata format:"
         << endl;
    RETURN_NOT_OK(PrintKuduAndHmsTables(master_addrs, &report.legacy_hms_tables, cout));
    cout << endl
         << "Suggestion: use the fix tool to upgrade the legacy metadata:" << endl
         << "\t$ kudu hms fix " << master_addrs << endl
         << endl;
  }

  if (!report.inconsistent_tables.empty()) {
    cout << "Found table(s) with inconsistent metadata:" << endl;
    RETURN_NOT_OK(PrintKuduAndHmsTables(master_addrs, &report.inconsistent_tables, cout));
    cout << endl
         << "Suggestion: use the fix tool to correct the inconsistent metadata:" << endl
         << "\t$ kudu hms fix " << master_addrs << endl
         << endl;
  }

  if (report.orphan_hms_tables.empty() &&
      report.legacy_hms_tables.empty() &&
      report.duplicate_hms_tables.empty() &&
      report.inconsistent_tables.empty() &&
      report.missing_hms_tables.empty() &&
      report.invalid_name_tables.empty()) {
    return Status::OK();
  }

  // TODO(dan): add a link to the HMS guide on kudu.apache.org to this message.
  return Status::IllegalState("found inconsistencies in the Kudu and HMS catalogs");
}

// Pretty-prints the table name and ID.
string TableIdent(const KuduTable& table) {
  return Substitute("$0 [id=$1]", table.name(), table.id());
}

// Analyzes the Kudu and HMS catalogs and attempts to fix any
// automatically-fixable issues.
//
// Error handling: unexpected errors (e.g. networking errors) are fatal and
// result in returning early. Expected application failures such as a rename
// failing due to duplicate table being present are logged and execution
// continues.
Status FixHmsMetadata(const RunnerContext& context) {
  const string& master_addrs = FindOrDie(context.required_args, kMasterAddressesArg);
  shared_ptr<KuduClient> kudu_client;
  unique_ptr<HmsCatalog> hms_catalog;
  RETURN_NOT_OK(Init(context, &kudu_client, &hms_catalog));

  CatalogReport report;
  RETURN_NOT_OK(AnalyzeCatalogs(master_addrs, hms_catalog.get(), kudu_client.get(), &report));

  bool success = true;

  if (FLAGS_drop_orphan_hms_tables) {
    for (hive::Table& hms_table : report.orphan_hms_tables) {
      string table_name = Substitute("$0.$1", hms_table.dbName, hms_table.tableName);
      const string& master_addrs_param = hms_table.parameters[HmsClient::kKuduMasterAddrsKey];
      if (master_addrs_param != master_addrs && !FLAGS_force) {
        LOG(INFO) << "Skipping drop of orphan HMS table " << table_name
                  << " with master addresses parameter " << master_addrs_param
                  << " because it does not match the --" << kMasterAddressesArg << " argument"
                  << " (use --force to skip this check)";
        continue;
      }

      if (FLAGS_dryrun) {
        LOG(INFO) << "[dryrun] Dropping orphan HMS table " << table_name;
      } else {
        const string& table_id = hms_table.parameters[HmsClient::kKuduTableIdKey];
        const string& storage_handler = hms_table.parameters[HmsClient::kStorageHandlerKey];
        // All errors are fatal here, since we've already checked that the table exists in the HMS.
        if (storage_handler == HmsClient::kKuduStorageHandler) {
          RETURN_NOT_OK_PREPEND(hms_catalog->DropTable(table_id, table_name),
              Substitute("failed to drop orphan HMS table $0", table_name));
        } else {
          RETURN_NOT_OK_PREPEND(hms_catalog->DropLegacyTable(table_name),
              Substitute("failed to drop legacy orphan HMS table $0", table_name));
        }
      }
    }
  }

  if (FLAGS_create_missing_hms_tables) {
    for (const auto& kudu_table : report.missing_hms_tables) {
      const string& table_id = kudu_table->id();
      const string& table_name = kudu_table->name();
      Schema schema = client::SchemaFromKuduSchema(kudu_table->schema());
      string normalized_table_name(table_name.data(), table_name.size());
      CHECK_OK(hms::HmsCatalog::NormalizeTableName(&normalized_table_name));

      if (FLAGS_dryrun) {
        LOG(INFO) << "[dryrun] Creating HMS table for Kudu table " << TableIdent(*kudu_table);
      } else {
        Status s = hms_catalog->CreateTable(table_id, table_name, schema);
        if (s.IsAlreadyPresent()) {
          LOG(ERROR) << "Failed to create HMS table for Kudu table "
                     << TableIdent(*kudu_table)
                     << " because another table already exists in the HMS with that name";
          success = false;
          continue;
        }
        if (s.IsInvalidArgument()) {
          // This most likely means the database doesn't exist, but it is ambiguous.
          LOG(ERROR) << "Failed to create HMS table for Kudu table "
                     << TableIdent(*kudu_table)
                     << " (database does not exist?): " << s.message().ToString();
          success = false;
          continue;
        }
        // All other errors are unexpected.
        RETURN_NOT_OK_PREPEND(s,
            Substitute("failed to create HMS table for Kudu table $0", TableIdent(*kudu_table)));
      }

      if (normalized_table_name != table_name) {
        if (FLAGS_dryrun) {
          LOG(INFO) << "[dryrun] Renaming Kudu table " << TableIdent(*kudu_table)
                    << " to lowercased Hive-compatible name: " << normalized_table_name;
        } else {
          // All errors are fatal. We never expect to get an 'AlreadyPresent'
          // error, since the catalog manager validates that no two
          // Hive-compatible table names differ only by case.
          //
          // Note that if an error occurs we do not roll-back the HMS table
          // creation step, since a subsequent run of the tool will recognize
          // the table as an inconsistent table (Kudu and HMS table names do not
          // match), and automatically fix it.
          RETURN_NOT_OK_PREPEND(
              RenameTableInKuduCatalog(kudu_client.get(), table_name, normalized_table_name),
              Substitute("failed to rename Kudu table $0 to lowercased Hive compatible name $1",
                         TableIdent(*kudu_table), normalized_table_name));
        }
      }
    }
  }

  if (FLAGS_upgrade_hms_tables) {
    for (const auto& table_pair : report.legacy_hms_tables) {
      const KuduTable& kudu_table = *table_pair.first;
      const hive::Table& hms_table = table_pair.second;
      string hms_table_name = Substitute("$0.$1", hms_table.dbName, hms_table.tableName);

      if (FLAGS_dryrun) {
        LOG(INFO) << "[dryrun] Upgrading legacy Impala HMS metadata for table "
                  << hms_table_name;
      } else {
        RETURN_NOT_OK_PREPEND(hms_catalog->UpgradeLegacyImpalaTable(
                  kudu_table.id(), hms_table.dbName, hms_table.tableName,
                  client::SchemaFromKuduSchema(kudu_table.schema())),
            Substitute("failed to upgrade legacy Impala HMS metadata for table $0",
              hms_table_name));
      }

      if (kudu_table.name() != hms_table_name) {
        if (FLAGS_dryrun) {
          LOG(INFO) << "[dryrun] Renaming Kudu table " << TableIdent(kudu_table)
                    << " to " << hms_table_name;
        } else {
          Status s = RenameTableInKuduCatalog(kudu_client.get(), kudu_table.name(), hms_table_name);
          if (s.IsAlreadyPresent()) {
            LOG(ERROR) << "Failed to rename Kudu table " << TableIdent(kudu_table)
                       << " to match the Hive Metastore name " << hms_table_name
                       << ", because a Kudu table with name" << hms_table_name
                       << " already exists";
            LOG(INFO) << "Suggestion: rename the conflicting table name manually:\n"
                      << "\t$ kudu table rename_table --alter_external_catalogs=false "
                      << master_addrs << " " << hms_table_name << " <database-name>.<table-name>'";
            success = false;
            continue;
          }

          // All other errors are fatal. Note that if an error occurs we do not
          // roll-back the HMS legacy upgrade step, since a subsequent run of
          // the tool will recognize the table as an inconsistent table (Kudu
          // and HMS table names do not match), and automatically fix it.
          RETURN_NOT_OK_PREPEND(s,
              Substitute("failed to rename Kudu table $0 to $1",
                TableIdent(kudu_table), hms_table_name));
        }
      }
    }
  }

  if (FLAGS_fix_inconsistent_tables) {
    for (const auto& table_pair : report.inconsistent_tables) {
      const KuduTable& kudu_table = *table_pair.first;
      const hive::Table& hms_table = table_pair.second;
      string hms_table_name = Substitute("$0.$1", hms_table.dbName, hms_table.tableName);

      if (hms_table_name != kudu_table.name()) {
        // Update the Kudu table name to match the HMS table name.
        if (FLAGS_dryrun) {
          LOG(INFO) << "[dryrun] Renaming Kudu table " << TableIdent(kudu_table)
                    << " to " << hms_table_name;
        } else {
          Status s = RenameTableInKuduCatalog(kudu_client.get(), kudu_table.name(), hms_table_name);
          if (s.IsAlreadyPresent()) {
            LOG(ERROR) << "Failed to rename Kudu table " << TableIdent(kudu_table)
                       << " to match HMS table " << hms_table_name
                       << ", because a Kudu table with name " << hms_table_name
                       << " already exists";
            success = false;
            continue;
          }
          RETURN_NOT_OK_PREPEND(s,
              Substitute("failed to rename Kudu table $0 to $1",
                TableIdent(kudu_table), hms_table_name));
        }
      }

      // Update the HMS table metadata to match Kudu.
      if (FLAGS_dryrun) {
        LOG(INFO) << "[dryrun] Refreshing HMS table metadata for Kudu table "
                  << TableIdent(kudu_table);
      } else {
        Schema schema(client::SchemaFromKuduSchema(kudu_table.schema()));
        RETURN_NOT_OK_PREPEND(
            hms_catalog->AlterTable(kudu_table.id(), hms_table_name, hms_table_name, schema),
            Substitute("failed to refresh HMS table metadata for Kudu table $0",
              TableIdent(kudu_table)));
      }
    }
  }

  if (FLAGS_dryrun || success) {
    return Status::OK();
  }
  return Status::RuntimeError("Failed to fix some catalog metadata inconsistencies");
}

unique_ptr<Mode> BuildHmsMode() {

  // TODO(dan): automatically retrieve the HMS URIs and SASL config from the
  // Kudu master instead of requiring them as an additional flag.

  unique_ptr<Action> hms_check =
      ActionBuilder("check", &CheckHmsMetadata)
          .Description("Check metadata consistency between Kudu and the Hive Metastore catalogs")
          .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
          .AddOptionalParameter("hive_metastore_uris")
          .AddOptionalParameter("hive_metastore_sasl_enabled")
          .Build();

  unique_ptr<Action> hms_fix =
    ActionBuilder("fix", &FixHmsMetadata)
        .Description("Fix automatically-repairable metadata inconsistencies in the "
                     "Kudu and Hive Metastore catalogs")
        .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
        .AddOptionalParameter("hive_metastore_uris")
        .AddOptionalParameter("hive_metastore_sasl_enabled")
        .AddOptionalParameter("dryrun")
        .AddOptionalParameter("drop_orphan_hms_tables")
        .AddOptionalParameter("create_missing_hms_tables")
        .AddOptionalParameter("fix_inconsistent_tables")
        .AddOptionalParameter("upgrade_hms_tables")
        .Build();

  // TODO(dan): add 'hms precheck' tool to check for overlapping normalized table names.

  unique_ptr<Action> hms_downgrade =
      ActionBuilder("downgrade", &HmsDowngrade)
          .Description("Downgrade the metadata to legacy format for "
                       "Kudu and the Hive Metastores")
          .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
          .AddOptionalParameter("hive_metastore_uris")
          .AddOptionalParameter("hive_metastore_sasl_enabled")
          .Build();

  return ModeBuilder("hms").Description("Operate on remote Hive Metastores")
                           .AddAction(std::move(hms_downgrade))
                           .AddAction(std::move(hms_check))
                           .AddAction(std::move(hms_fix))
                           .Build();
}

} // namespace tools
} // namespace kudu
