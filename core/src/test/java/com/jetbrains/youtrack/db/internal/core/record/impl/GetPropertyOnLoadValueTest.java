package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class GetPropertyOnLoadValueTest extends DbTestBase {

  @Test
  public void testOnloadValue() {
    db.createClass("test");
    db.begin();
    EntityImpl doc = new EntityImpl("test");
    doc.setProperty("name", "John Doe");
    doc.save();
    db.commit();
    RID id = doc.getIdentity();
    db.activateOnCurrentThread();
    db.begin();
    EntityImpl doc2 = db.load(id);
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
    Vertex doc = db.newVertex("test");

    IntStream.rangeClosed(1, 8)
        .forEach(
            i -> {
              Vertex linked = db.newVertex("test");
              linked.setProperty("name", i + "");
              doc.addEdge(linked, "myLink");
            });
    doc.save();
    db.commit();

    db.begin();
    var loadedDoc = db.<Entity>load(doc.getIdentity());
    loadedDoc.getPropertyOnLoadValue(Vertex.DIRECTION_OUT_PREFIX + "myLink");
  }

  @Test
  public void testOnLoadValueForScalarList() throws IllegalArgumentException {
    db.createVertexClass("test");
    db.begin();
    Vertex doc = db.newVertex("test");
    doc.setProperty("list", Arrays.asList(1, 2, 3));
    doc.save();
    db.commit();
    db.begin();
    doc = db.load(doc.getIdentity());
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
    Vertex doc = db.newVertex("test");
    doc.setProperty("set", new HashSet<>(Arrays.asList(1, 2, 3)));
    doc.save();
    db.commit();
    db.begin();
    doc = db.load(doc.getIdentity());
    Set<Integer> storedSet = doc.getProperty("set");
    storedSet.add(4);
    doc.save();
    Set<Integer> onLoad = doc.getPropertyOnLoadValue("set");
    Assert.assertEquals(3, onLoad.size());
  }

  @Test
  public void testStringBlobOnLoadValue() {
    String before = "Hello World";
    var byteArrayBefore = before.getBytes();
    String after = "Goodbye Cruel World";

    var byteArrayAfter = after.getBytes();
    var oBlob = new RecordBytes(byteArrayBefore);
    var oBlob2 = new RecordBytes(byteArrayAfter);
    db.createVertexClass("test");
    db.begin();
    VertexEntityImpl doc = (VertexEntityImpl) db.newVertex("test");
    doc.setProperty("stringBlob", oBlob);
    doc.save();
    db.commit();
    db.begin();

    doc = db.bindToSession(doc);
    doc.setLazyLoad(true);
    doc = db.load(doc.getIdentity());
    doc.setProperty("stringBlob", oBlob2);
    doc.save();
    RecordBytes onLoad = doc.getPropertyOnLoadValue("stringBlob");
    Assert.assertEquals(before, new String(onLoad.toStream()));
    Assert.assertEquals(
        after, new String(((RecordBytes) doc.getProperty("stringBlob")).toStream()));
    // no lazy load
    doc.setLazyLoad(false);
    Assert.assertTrue(doc.getPropertyOnLoadValue("stringBlob") instanceof RID);
  }

  @Test
  public void testRandomOnLoadValue() {
    var seed = System.currentTimeMillis();
    System.out.println("Seed is " + seed);
    var random = new Random(seed);
    var propertyNames = Arrays.asList("prop1", "prop2", "prop3", "prop4");
    var values = new ArrayList<Object>();
    values.add(100);
    values.add(1000L);
    values.add(100.0);
    values.add(500.0d);
    values.add((byte) 0xf0);
    values.add("Hello");
    values.add(new HashSet<>(Arrays.asList(99, 22, 21)));
    values.add(Arrays.asList(1, 2, 3, 4));
    values.add(Arrays.asList("1", "2", "3", "4"));

    var operations = new ArrayList<BiFunction<Vertex, String, Void>>();
    operations.add(
        (oVertex, propertyName) -> {
          oVertex.setProperty(propertyName, values.get(random.nextInt(values.size())));
          oVertex.save();
          return null;
        });
    operations.add(
        (oVertex, propertyName) -> {
          if (oVertex.hasProperty(propertyName)) {
            oVertex.removeProperty(propertyName);
          }
          oVertex.save();
          return null;
        });

    db.createVertexClass("test");
    db.begin();
    Vertex doc = db.newVertex("test");
    var initialValues = new HashMap<String, Object>();
    propertyNames.forEach(
        name -> {
          var value = values.get(random.nextInt(values.size()));
          doc.setProperty(name, value);
        });

    doc.save();
    db.commit();
    for (var txId = 0; txId < 5; txId++) {
      db.begin();
      var boundDoc = db.bindToSession(doc);
      propertyNames.forEach(name -> initialValues.put(name, boundDoc.getProperty(name)));
      for (var i = 0; i < 1000; i++) {
        var operation = operations.get(random.nextInt(operations.size()));
        var propertyName = propertyNames.get(random.nextInt(propertyNames.size()));
        operation.apply(boundDoc, propertyName);
        assertInitialValues(boundDoc, initialValues);
      }
      db.commit();
    }
  }

  private void assertInitialValues(Vertex vertex, Map<String, Object> initialValues) {
    initialValues.forEach(
        (key, value) -> {
          Assert.assertEquals(vertex.getPropertyOnLoadValue(key), value);
        });
  }
}
