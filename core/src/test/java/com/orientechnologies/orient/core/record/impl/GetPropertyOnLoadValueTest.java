package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class GetPropertyOnLoadValueTest extends BaseMemoryDatabase {

  @Test
  public void testOnloadValue() {
    db.createClass("test");
    db.begin();
    ODocument doc = new ODocument("test");
    doc.setProperty("name", "John Doe");
    doc.save();
    db.commit();
    ORID id = doc.getIdentity();
    db.activateOnCurrentThread();
    db.begin();
    ODocument doc2 = db.load(id);
    doc2.setProperty("name", "Sun Doe");
    doc2.save();
    doc2.setProperty("name", "Jane Doe");
    doc2.save();
    Assert.assertEquals("John Doe", doc2.getPropertyOnLoadValue("name"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOnLoadValueForList() throws IllegalArgumentException {
    db.createVertexClass("test");
    db.createEdgeClass("myLink");
    db.begin();
    OVertex doc = db.newVertex("test");

    IntStream.rangeClosed(1, 8)
        .forEach(i -> {
          OVertex linked = db.newVertex("test");
          linked.setProperty("name", i + "");
          doc.addEdge(linked, "myLink");
        });
    doc.save();
    db.commit();
    db.begin();
    doc.load();
    doc.getPropertyOnLoadValue(OVertex.DIRECTION_OUT_PREFIX + "myLink");
  }

  @Test
  public void testOnLoadValueForScalarList() throws IllegalArgumentException {
    db.createVertexClass("test");
    db.begin();
    OVertex doc = db.newVertex("test");
    doc.setProperty("list", Arrays.asList(1, 2, 3));
    doc.save();
    db.commit();
    db.begin();
    doc.load();
    List<Integer> storedList = doc.getProperty("list");
    storedList.add(4);
    doc.save();
    List<Integer> onLoad = doc.getPropertyOnLoadValue("list");
    Assert.assertEquals(3, onLoad.size());
  }

  @Test
  public void testOnLoadValueForScalarSet() throws IllegalArgumentException {
    db.createVertexClass("test");
    db.begin();
    OVertex doc = db.newVertex("test");
    doc.setProperty("set", new HashSet<>(Arrays.asList(1, 2, 3)));
    doc.save();
    db.commit();
    db.begin();
    doc.load();
    Set<Integer> storedSet = doc.getProperty("set");
    storedSet.add(4);
    doc.save();
    Set<Integer> onLoad = doc.getPropertyOnLoadValue("set");
    Assert.assertEquals(3, onLoad.size());
  }


}
