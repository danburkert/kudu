package org.apache.kudu.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;

/**
 * {@code KuduMetastorePlugin} intercepts DDL operations on Kudu table entries
 * in the HMS, and validates that they are consistent.
 */
public class KuduMetastorePlugin extends MetaStoreEventListener {

  private static final String KUDU_STORAGE_HANDLER = "org.apache.kudu.hive.KuduStorageHandler";
  private static final String KUDU_TABLE_ID_KEY = "kudu.table_id";
  private static final String KUDU_MASTER_ADDRS_KEY = "kudu.master_addresses";

  public KuduMetastorePlugin(Configuration config) {
    super(config);
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    super.onCreateTable(tableEvent);
    Table table = tableEvent.getTable();

    // Allow non-Kudu tables to be created.
    if (!isKuduTable(table)) {
      // But ensure that the new table does not contain Kudu-specific properties.
      checkNoKuduProperties(table);
      return;
    }

    checkKuduProperties(table);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    super.onDropTable(tableEvent);

    EnvironmentContext environmentContext = tableEvent.getEnvironmentContext();
    String targetTableId = environmentContext == null ? null :
        environmentContext.getProperties().get(KUDU_TABLE_ID_KEY);

    // If this request doesn't specify a Kudu table ID then allow it to proceed.
    if (targetTableId == null) {
      return;
    }

    Table table = tableEvent.getTable();

    // Check that the table being dropped is a Kudu table.
    if (!isKuduTable(table)) {
      throw new MetaException("Kudu table ID does not match the non-Kudu HMS entry");
    }

    // Check that the table's ID matches the request's table ID.
    if (!targetTableId.equals(table.getParameters().get(KUDU_TABLE_ID_KEY))) {
      throw new MetaException("Kudu table ID does not match the HMS entry");
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    super.onAlterTable(tableEvent);

    Table oldTable = tableEvent.getOldTable();
    Table newTable = tableEvent.getNewTable();

    // Allow non-Kudu tables to be altered.
    if (!isKuduTable(oldTable)) {
      // But ensure that the alteration isn't introducing Kudu-specific properties.
      checkNoKuduProperties(newTable);
      return;
    }

    // Check the altered table's properties.
    checkKuduProperties(newTable);

    // Check that the table ID isn't changing.
    String oldTableId = oldTable.getParameters().get(KUDU_TABLE_ID_KEY);
    String newTableId = newTable.getParameters().get(KUDU_TABLE_ID_KEY);
    if (!newTableId.equals(oldTableId)) {
      throw new MetaException("Kudu table ID does not match the existing HMS entry");
    }
  }

  /**
   * Checks whether the table is a Kudu table.
   * @param table the table to check
   * @return {@code true} if the table is a Kudu table, otherwise {@code false}
   */
  private boolean isKuduTable(Table table) {
    return table.getParameters()
                .get(hive_metastoreConstants.META_TABLE_STORAGE)
                .equals(KUDU_STORAGE_HANDLER);
  }

  /**
   * Checks that the Kudu table entry contains the required Kudu table properties.
   * @param table the table to check
   */
  private void checkKuduProperties(Table table) throws MetaException {
    if (!isKuduTable(table)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain the Kudu storage handler property (%s=%s)",
          hive_metastoreConstants.META_TABLE_STORAGE,
          KUDU_STORAGE_HANDLER));
    }
    String tableId = table.getParameters().get(KUDU_TABLE_ID_KEY);
    if (tableId == null || tableId.isEmpty()) {
      throw new MetaException(String.format(
          "Kudu table entry must contain a table ID property (%s)", KUDU_TABLE_ID_KEY));
    }
    String masterAddresses = table.getParameters().get(KUDU_MASTER_ADDRS_KEY);
    if (masterAddresses == null || masterAddresses.isEmpty()) {
      throw new MetaException(String.format(
          "Kudu table entry must contain a Master addresses property (%s)", KUDU_MASTER_ADDRS_KEY));
    }
  }

  /**
   * Checks that the non-Kudu table entry does not contain Kudu-specific table properties.
   * @param table the table to check
   */
  private void checkNoKuduProperties(Table table) throws MetaException {
    if (isKuduTable(table)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain the Kudu storage handler (%s=%s)",
          hive_metastoreConstants.META_TABLE_STORAGE,
          KUDU_STORAGE_HANDLER));
    }
    if (table.getParameters().containsKey(KUDU_TABLE_ID_KEY)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain a table ID property (%s)",
          KUDU_TABLE_ID_KEY));
    }
    if (table.getParameters().containsKey(KUDU_MASTER_ADDRS_KEY)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain a Master addresses property (%s)",
          KUDU_MASTER_ADDRS_KEY));
    }
  }
}
