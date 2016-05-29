package org.kududb.client;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.kududb.master.Master;
import org.kududb.util.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public final class MetaCache {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncKuduClient.class);

  private static final int MASTER_LEASES = 100;

  private final Object monitor = new Object();

  /**
   * Index of Tablet ID -> Tablet.
   */
  @GuardedBy("monitor")
  private final Map<Slice, RemoteTablet> tabletsById = new HashMap<>();

  /**
   * Index of Table ID -> Start Partition Key -> Tablet.
   */
  @GuardedBy("monitor")
  private final Map<String, NavigableMap<byte[], RemoteTablet>> tabletsByTableAndPartition = new HashMap<>();

  /**
   * Look up which tablet hosts the given partition key for a table.
   * Only tablets with non-failed LEADERs are considered.
   *
   * @param table the table to which the tablet belongs
   * @param partitionKey a partition key whose tablet is being looked up
   * @param timeout operation timeout in milliseonds
   * @return the tablet
   * @throws PleaseThrottleException
   */
  public Deferred<RemoteTablet> lookupTablet(KuduTable table,
                                             byte[] partitionKey,
                                             long timeout) throws PleaseThrottleException {
    RemoteTablet tablet = lookupFastpath(table, partitionKey);
    if (tablet != null) return Deferred.fromResult(tablet);

    GetTableLocationsRequest rpc = new GetTableLocationsRequest(table, partitionKey, null);
    rpc.setTimeoutMillis(timeout);



    synchronized (monitor) {

      if (outstandingRequests.size() > MASTER_LEASES) {
        outstandingRequests.



      }

    }



  }


  private class GetTableLocationsCB implements Callback<Void, Master.GetTableLocationsResponsePB> {
    @Override
    public Void call(Master.GetTableLocationsResponsePB resp) throws Exception {
      if (resp.hasError()) {
        LOG.warn("unable to lookup table locations: {}", resp.getError());
        return null;
      }

      return null;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .toString();
    }
  }

  /**
   * Looks up a tablet by partition key in the local cache, returning {@code null}
   * if the local cache does not have an entry for it.
   *
   * @param table the table name
   * @param partitionKey the partition key
   * @return the remote tablet, or {@code null} if it is not cached
   */
  private RemoteTablet lookupFastpath(KuduTable table, byte[] partitionKey) {
    final RemoteTablet tablet;
    synchronized (monitor) {
      NavigableMap<byte[], RemoteTablet> tabletsByPartition =
          tabletsByTableAndPartition.get(table.getTableId());
      if (tabletsByPartition == null) return null;

      tablet = tabletsByPartition.floorEntry(partitionKey).getValue();
    }
    if (tablet == null) return null;

    // If the partition is not the end partition, but it doesn't include the key
    // we are looking for, then we have not yet found the correct tablet.
    if (!tablet.getPartition().isEndPartition()
        && Bytes.memcmp(partitionKey, tablet.getPartition().getPartitionKeyEnd()) >= 0) {
      return null;
    }

    return tablet;
  }



  @ThreadSafe
  public static final class RemoteTablet {
    private static final int NO_LEADER_INDEX = -1;
    private final String tableId;
    private final Slice tabletId;
    private final Partition partition;

    @GuardedBy("tabletServers")
    private final ArrayList<TabletClient> tabletServers = new ArrayList<TabletClient>();
    @GuardedBy("tabletServers")
    private int leaderIndex = NO_LEADER_INDEX;

    public RemoteTablet(String tableId, Slice tabletId, Partition partition) {
      this.tableId = tableId;
      this.tabletId = tabletId;
      this.partition = partition;
    }

    public String getTableId() {
      return tableId;
    }

    public Slice getTabletId() {
      return tabletId;
    }

    public Partition getPartition() {
      return partition;
    }
  }
}
