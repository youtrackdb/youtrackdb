package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ORidSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class ORidSetTest extends OParserTestAbstract {

  @Test
  public void testPut() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut0() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 0);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut31() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut32() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 32);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut63() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 63);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut64() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 64);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut65() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 65);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testRemove() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testBigClusterId() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(1200, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testBigClusterPosition() {
    ORidSet set = new ORidSet();
    YTRID rid = new YTRecordId(12, 200L * 1000 * 1000);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testIterator() {

    Set<YTRID> set = new ORidSet();
    int clusters = 100;
    int idsPerCluster = 10;

    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        set.add(new YTRecordId(cluster, id));
      }
    }
    Iterator<YTRID> iterator = set.iterator();

    System.out.println("stating");
    long begin = System.currentTimeMillis();
    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        Assert.assertTrue(iterator.hasNext());
        YTRID next = iterator.next();
        Assert.assertNotNull(next);
        //        Assert.assertEquals(new YTRecordId(cluster, id), next);
      }
    }
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorOffset() {

    Set<YTRID> control = new HashSet<>();
    Set<YTRID> set = new ORidSet();

    long offset = (((long) Integer.MAX_VALUE)) * 63;
    long idsPerCluster = 10;

    int cluster = 1;
    for (long id = 0; id < idsPerCluster; id++) {
      YTRecordId rid = new YTRecordId(cluster, offset + id);
      set.add(rid);
      control.add(rid);
    }
    Iterator<YTRID> iterator = set.iterator();

    for (long id = 0; id < idsPerCluster; id++) {
      Assert.assertTrue(iterator.hasNext());
      YTRID next = iterator.next();
      Assert.assertNotNull(next);
      control.remove(next);
    }

    Assert.assertFalse(iterator.hasNext());
    Assert.assertTrue(control.isEmpty());
  }
}
