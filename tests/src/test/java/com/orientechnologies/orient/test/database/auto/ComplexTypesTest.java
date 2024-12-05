/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
@Test
public class ComplexTypesTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public ComplexTypesTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testBigDecimal() {
    YTEntityImpl newDoc = new YTEntityImpl();
    newDoc.field("integer", new BigInteger("10"));
    newDoc.field("decimal_integer", new BigDecimal(10));
    newDoc.field("decimal_float", new BigDecimal("10.34"));

    database.begin();
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertEquals(((Number) loadedDoc.field("integer")).intValue(), 10);
    Assert.assertEquals(loadedDoc.field("decimal_integer"), new BigDecimal(10));
    Assert.assertEquals(loadedDoc.field("decimal_float"), new BigDecimal("10.34"));
  }

  @Test
  public void testEmbeddedList() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final ArrayList<YTEntityImpl> list = new ArrayList<YTEntityImpl>();
    newDoc.field("embeddedList", list, YTType.EMBEDDEDLIST);
    list.add(new YTEntityImpl().field("name", "Luca"));
    list.add(new YTEntityImpl("Account").field("name", "Marcus"));

    database.begin();
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<YTEntityImpl>) loadedDoc.field("embeddedList")).get(0) instanceof YTEntityImpl);

    YTEntityImpl d = ((List<YTEntityImpl>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<YTEntityImpl>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(d.getClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testLinkList() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final ArrayList<YTEntityImpl> list = new ArrayList<YTEntityImpl>();
    newDoc.field("linkedList", list, YTType.LINKLIST);
    database.begin();

    var doc = new YTEntityImpl();
    doc.field("name", "Luca")
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    list.add(doc);

    list.add(new YTEntityImpl("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedList"));
    Assert.assertTrue(loadedDoc.field("linkedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<YTIdentifiable>) loadedDoc.field("linkedList")).get(0) instanceof YTIdentifiable);

    YTEntityImpl d = ((List<YTIdentifiable>) loadedDoc.field("linkedList")).get(0).getRecord();
    Assert.assertTrue(d.getIdentity().isValid());
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<YTIdentifiable>) loadedDoc.field("linkedList")).get(1).getRecord();
    Assert.assertEquals(d.getClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testEmbeddedSet() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    newDoc.field("embeddedSet", set, YTType.EMBEDDEDSET);
    set.add(new YTEntityImpl().field("name", "Luca"));
    set.add(new YTEntityImpl("Account").field("name", "Marcus"));

    database.begin();
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedSet"));
    Assert.assertTrue(loadedDoc.field("embeddedSet", Set.class) instanceof Set<?>);

    final Iterator<YTEntityImpl> it =
        ((Collection<YTEntityImpl>) loadedDoc.field("embeddedSet")).iterator();

    int tot = 0;
    while (it.hasNext()) {
      YTEntityImpl d = it.next();
      Assert.assertTrue(d instanceof YTEntityImpl);

      if (d.field("name").equals("Marcus")) {
        Assert.assertEquals(d.getClassName(), "Account");
      }

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testLinkSet() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final Set<YTEntityImpl> set = new HashSet<YTEntityImpl>();
    newDoc.field("linkedSet", set, YTType.LINKSET);
    database.begin();
    var doc = new YTEntityImpl();
    doc.field("name", "Luca")
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    set.add(doc);

    set.add(new YTEntityImpl("Account").field("name", "Marcus"));

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedSet"));
    Assert.assertTrue(loadedDoc.field("linkedSet", Set.class) instanceof Set<?>);

    final Iterator<YTIdentifiable> it =
        ((Collection<YTIdentifiable>) loadedDoc.field("linkedSet")).iterator();

    int tot = 0;
    while (it.hasNext()) {
      var d = it.next().getEntity();

      if (Objects.equals(d.getProperty("name"), "Marcus")) {
        Assert.assertEquals(d.getClassName(), "Account");
      }

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testEmbeddedMap() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    newDoc.field("embeddedMap", map, YTType.EMBEDDEDMAP);
    map.put("Luca", new YTEntityImpl().field("name", "Luca"));
    map.put("Marcus", new YTEntityImpl().field("name", "Marcus"));
    map.put("Cesare", new YTEntityImpl("Account").field("name", "Cesare"));

    database.begin();
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, YTEntityImpl>) loadedDoc.field("embeddedMap")).values().iterator().next()
            instanceof YTEntityImpl);

    YTEntityImpl d = ((Map<String, YTEntityImpl>) loadedDoc.field("embeddedMap")).get("Luca");
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, YTEntityImpl>) loadedDoc.field("embeddedMap")).get("Marcus");
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, YTEntityImpl>) loadedDoc.field("embeddedMap")).get("Cesare");
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getClassName(), "Account");
  }

  @Test
  public void testEmptyEmbeddedMap() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    newDoc.field("embeddedMap", map, YTType.EMBEDDEDMAP);

    database.begin();
    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);

    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, YTEntityImpl> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testLinkMap() {
    YTEntityImpl newDoc = new YTEntityImpl();

    final Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    newDoc.field("linkedMap", map, YTType.LINKMAP);
    database.begin();
    var doc1 = new YTEntityImpl();
    doc1.field("name", "Luca")
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    map.put("Luca", doc1);
    var doc2 = new YTEntityImpl();
    doc2.field("name", "Marcus")
        .save(database.getClusterNameById(database.getDefaultClusterId()));
    map.put("Marcus", doc2);

    var doc3 = new YTEntityImpl("Account");
    doc3.field("name", "Cesare").save();
    map.put("Cesare", doc3);

    database.save(newDoc, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final YTRID rid = newDoc.getIdentity();

    database.close();
    database = acquireSession();

    YTEntityImpl loadedDoc = database.load(rid);
    Assert.assertNotNull(loadedDoc.field("linkedMap", YTType.LINKMAP));
    Assert.assertTrue(loadedDoc.field("linkedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, YTIdentifiable>) loadedDoc.field("linkedMap")).values().iterator().next()
            instanceof YTIdentifiable);

    YTEntityImpl d =
        ((Map<String, YTIdentifiable>) loadedDoc.field("linkedMap")).get("Luca").getRecord();
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, YTIdentifiable>) loadedDoc.field("linkedMap")).get("Marcus").getRecord();
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, YTIdentifiable>) loadedDoc.field("linkedMap")).get("Cesare").getRecord();
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getClassName(), "Account");
  }
}
