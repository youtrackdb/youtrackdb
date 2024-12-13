package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.sql.executor.RidSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class RidSetTest extends ParserTestAbstract {

  @Test
  public void testPut() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut0() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 0);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut31() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut32() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 32);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut63() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 63);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut64() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 64);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testPut65() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 65);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test
  public void testRemove() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testBigClusterId() {
    RidSet set = new RidSet();
    RID rid = new RecordId(1200, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testBigClusterPosition() {
    RidSet set = new RidSet();
    RID rid = new RecordId(12, 200L * 1000 * 1000);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test
  public void testIterator() {

    Set<RID> set = new RidSet();
    int clusters = 100;
    int idsPerCluster = 10;

    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        set.add(new RecordId(cluster, id));
      }
    }
    Iterator<RID> iterator = set.iterator();

    System.out.println("stating");
    long begin = System.currentTimeMillis();
    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        Assert.assertTrue(iterator.hasNext());
        RID next = iterator.next();
        Assert.assertNotNull(next);
        //        Assert.assertEquals(new RecordId(cluster, id), next);
      }
    }
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorOffset() {

    Set<RID> control = new HashSet<>();
    Set<RID> set = new RidSet();

    long offset = (((long) Integer.MAX_VALUE)) * 63;
    long idsPerCluster = 10;

    int cluster = 1;
    for (long id = 0; id < idsPerCluster; id++) {
      RecordId rid = new RecordId(cluster, offset + id);
      set.add(rid);
      control.add(rid);
    }
    Iterator<RID> iterator = set.iterator();

    for (long id = 0; id < idsPerCluster; id++) {
      Assert.assertTrue(iterator.hasNext());
      RID next = iterator.next();
      Assert.assertNotNull(next);
      control.remove(next);
    }

    Assert.assertFalse(iterator.hasNext());
    Assert.assertTrue(control.isEmpty());
  }
}
