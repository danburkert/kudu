package org.kududb.ts;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.kududb.client.BaseKuduTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTags extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTSDB.class);

  /** Builds a tagset from the provided tags. */
  private static Messages.Tagset tagset(String... tags) {
    Preconditions.checkArgument(tags.length % 2 == 0, "missing tag value");
    Messages.Tagset.Builder tagset = Messages.Tagset.newBuilder();
    for (int i = 0; i < tags.length; i += 2) {
      tagset.addTagsBuilder().setKey(tags[i]).setValue(tags[i+1]);
    }
    return tagset.build();
  }
  private static Deferred<List<Integer>> idSet(Collection<Integer> ids) {
    List<Integer> idSet = new ArrayList<>(ids);
    return Deferred.fromResult(idSet);
  }

  @Test
  public void testInsertAndGetTags() throws Exception {
    try (KuduTSDB tsdb = KuduTSDB.open(ImmutableList.of(getMasterAddresses()), "testInsertAndGetTags")) {
      Tags tags = tsdb.getTags();

      assertEquals(Integer.valueOf(0), tags.insertTagset(0, tagset("k1", "v1")).join());
      assertEquals(Integer.valueOf(1), tags.insertTagset(1, tagset("k1", "v2")).join());
      assertEquals(Integer.valueOf(2), tags.insertTagset(2, tagset("k2", "v1")).join());
      assertEquals(Integer.valueOf(3), tags.insertTagset(3, tagset("k2", "v2")).join());
      assertEquals(Integer.valueOf(4), tags.insertTagset(4, tagset("k1", "v1", "k2", "v1")).join());
      assertEquals(Integer.valueOf(5), tags.insertTagset(5, tagset("k1", "v1", "k2", "v2")).join());
      assertEquals(Integer.valueOf(6), tags.insertTagset(6, tagset("k1", "v2", "k2", "v1")).join());
      assertEquals(Integer.valueOf(7), tags.insertTagset(7, tagset("k1", "v2", "k2", "v2")).join());

      assertEquals(ImmutableList.of(0, 4, 5), tags.getTagsetIDsForTag("k1", "v1").join());
      assertEquals(ImmutableList.of(3, 5, 7), tags.getTagsetIDsForTag("k2", "v2").join());
      assertEquals(ImmutableList.of(), tags.getTagsetIDsForTag("k1", "v3").join());
    }
  }

  @Test
  public void testUnionTagsetIDs() throws Exception {
    List<Integer> emptySet = ImmutableList.of();
    List<Integer> set1 = ImmutableList.of(0, 9, 10, 12, 15, 2000);
    List<Integer> set2 = ImmutableList.of(0, 9, 100, 555);
    List<Integer> set3 = ImmutableList.of(12, 555);

    assertEquals(emptySet,
                 Tags.unionTagsetIDs(ImmutableList.<Deferred<List<Integer>>>of()).join());

    assertEquals(emptySet, Tags.unionTagsetIDs(ImmutableList.of(
        Deferred.fromResult(emptySet))).join());

    assertEquals(emptySet,
                 Tags.unionTagsetIDs(ImmutableList.of(idSet(emptySet), idSet(emptySet))).join());

    assertEquals(set1,
                 Tags.unionTagsetIDs(ImmutableList.of(idSet(emptySet), idSet(set1))).join());

    assertEquals(ImmutableList.of(0, 9, 10, 12, 15, 100, 555, 2000),
                 Tags.unionTagsetIDs(ImmutableList.of(idSet(set1),
                                                      idSet(set2),
                                                      idSet(emptySet),
                                                      idSet(set3))).join());
  }

  @Test
  public void testIntersectTagsetIDs() throws Exception {
    List<Integer> emptySet = ImmutableList.of();
    List<Integer> set1 = ImmutableList.of(0, 9, 10, 15, 2000);
    List<Integer> set2 = ImmutableList.of(0, 9, 100, 555);
    List<Integer> set3 = ImmutableList.of(12, 555);

    assertEquals(emptySet,
                 Tags.intersectTagsetIDs(ImmutableList.<Deferred<List<Integer>>>of()).join());

    assertEquals(emptySet, Tags.intersectTagsetIDs(ImmutableList.of(idSet(emptySet))).join());

    assertEquals(emptySet,
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(emptySet),
                                                          idSet(emptySet))).join());

    assertEquals(emptySet,
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(emptySet),
                                                          idSet(set1))).join());

    assertEquals(ImmutableList.of(555),
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(set2),
                                                          idSet(set3))).join());

    assertEquals(ImmutableList.of(0, 9),
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(set1),
                                                          idSet(set2))).join());

    assertEquals(ImmutableList.of(0, 9),
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(set2),
                                                          idSet(set1))).join());

    assertEquals(ImmutableList.of(),
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(set1),
                                                          idSet(set3))).join());

    assertEquals(ImmutableList.of(),
                 Tags.intersectTagsetIDs(ImmutableList.of(idSet(set1),
                                                          idSet(set2),
                                                          idSet(set3))).join());
  }
}