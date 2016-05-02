package org.kududb.ts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class TestIntVec {
  private static final Random RAND = new Random();

  private List<Integer> random() {
    return random(RAND.nextInt(1024));
  }

  private List<Integer> random(int len) {
    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      list.add(RAND.nextInt(i + 1));
    }
    return Collections.unmodifiableList(list);
  }

  public void checkIntVec(List<Integer> ints) {
    IntVec vec = IntVec.create();
    assertEquals(0, vec.len());

    // push
    for (int i : ints) {
      vec.push(i);
    }
    assertEquals(ints, vec.asList());

    // toString
    assertEquals(ints.toString(), vec.toString());

    // clone, equals
    IntVec copy = vec.clone();
    assertEquals(copy, vec);

    // truncate
    copy.truncate(vec.len() + 1);
    assertEquals(ints, copy.asList());
    vec.truncate(copy.len());
    assertEquals(ints, copy.asList());
    copy.truncate(ints.size() / 2);
    assertEquals(ints.subList(0, ints.size() / 2), copy.asList());

    // reserve
    int unused = copy.capacity() - copy.len();

    copy.reserve(unused);
    assertEquals(vec.capacity(), copy.capacity());

    copy.reserve(unused + 1);
    assertTrue(copy.capacity() > vec.capacity());

    copy.truncate(0);
    assertEquals(0, copy.len());

    // shrinkToFit
    copy.shrinkToFit();
    assertEquals(0, copy.capacity());
    vec.shrinkToFit();
    assertEquals(vec.len(), vec.capacity());

    // sort
    IntVec sorted = vec.clone();
    sorted.sort();
    List<Integer> sortedInts = new ArrayList<>(ints);
    Collections.sort(sortedInts);
    assertEquals(sortedInts, sorted.asList());

    // intersect
    for (int i = 0; i < 100; i++) {
      List<Integer> rand = random(i);

      IntVec a = IntVec.create();
      IntVec b = IntVec.create();
      for (int j : ints) a.push(j);
      for (int j : rand) b.push(j);
      a.sort();
      b.sort();

      IntVec left = a.clone();
      IntVec right = b.clone();

      left.intersect(b);
      right.intersect(a);

      SortedSet<Integer> expected =
          ImmutableSortedSet.copyOf(Sets.intersection(ImmutableSet.copyOf(ints),
                                                      ImmutableSet.copyOf(rand)));

      // There's no easy way to get a set intersection that preserves duplicates,
      // so we dedup. Intersections with duplicates are tested below.
      left.dedup();
      right.dedup();

      assertEquals(left, right);
      assertEquals(ImmutableList.copyOf(expected), left.asList());
    }

    // dedup
    sorted.dedup();
    assertEquals(ImmutableList.copyOf(new TreeSet<>(ints)), sorted.asList());

    // merge
    for (int i = 0; i < 100; i++) {
      List<Integer> rand = random(i);

      IntVec a = IntVec.create();
      IntVec b = IntVec.create();
      for (int j : ints) a.push(j);
      for (int j : rand) b.push(j);
      a.sort();
      b.sort();

      IntVec left = a.clone();
      IntVec right = b.clone();

      left.merge(b);
      right.merge(a);

      List<Integer> sortedRand = new ArrayList<>(rand);
      Collections.sort(sortedRand);
      Iterable<Integer> expected = Iterables.mergeSorted(ImmutableList.of(sortedInts, sortedRand),
                                                         new Comparator<Integer>() {
                                                           @Override
                                                           public int compare(Integer a, Integer b) {
                                                             return Ints.compare(a, b);
                                                           }
                                                         });

      assertEquals(left, right);
      assertEquals(ImmutableList.copyOf(expected), left.asList());
    }

    // get
    for (int i = 0; i < ints.size(); i++) {
      assertEquals(ints.get(i).intValue(), vec.get(i));
    }

    // set
    if (vec.len() > 0) {
      copy = vec.clone();
      int index = RAND.nextInt(vec.len());
      copy.set(index, index);
      List<Integer> intsCopy = new ArrayList<>(ints);
      intsCopy.set(index, index);
      assertEquals(intsCopy, copy.asList());
    }
  }

  @Test
  public void testIntVec() throws Exception {
    checkIntVec(random(0));
    checkIntVec(random(1));
    checkIntVec(random(2));
    checkIntVec(random(3));
    checkIntVec(random(IntVec.DEFAULT_CAPACITY - 2));
    checkIntVec(random(IntVec.DEFAULT_CAPACITY - 1));
    checkIntVec(random(IntVec.DEFAULT_CAPACITY));
    checkIntVec(random(IntVec.DEFAULT_CAPACITY + 1));
    checkIntVec(random(IntVec.DEFAULT_CAPACITY + 2));

    for (int i = 0; i < 100; i++) {
      checkIntVec(random());
    }
  }

  @Test
  public void testIntersectionWithDuplicates() throws Exception {
    IntVec a = IntVec.create();
    IntVec b = IntVec.create();

    a.push(0);
    a.push(1);
    a.push(1);
    b.push(1);
    b.push(1);
    b.push(3);

    IntVec left = a.clone();
    left.intersect(b);

    assertEquals(2, left.len());

    IntVec right = b.clone();
    right.intersect(a);

    assertEquals(left, right);
    assertEquals(ImmutableList.of(1, 1), left.asList());
  }
}
