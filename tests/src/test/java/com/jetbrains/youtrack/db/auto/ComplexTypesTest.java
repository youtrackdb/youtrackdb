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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
public class ComplexTypesTest extends BaseDBTest {

  @Parameters(value = "remote")
  public ComplexTypesTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testBigDecimal() {
    var newDoc = ((EntityImpl) session.newEntity());
    newDoc.field("integer", new BigInteger("10"));
    newDoc.field("decimal_integer", new BigDecimal(10));
    newDoc.field("decimal_float", new BigDecimal("10.34"));

    session.begin();
    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertEquals(((Number) loadedDoc.field("integer")).intValue(), 10);
    Assert.assertEquals(loadedDoc.field("decimal_integer"), new BigDecimal(10));
    Assert.assertEquals(loadedDoc.field("decimal_float"), new BigDecimal("10.34"));
  }

  @Test
  public void testEmbeddedList() {
    var newDoc = ((EntityImpl) session.newEntity());

    final var list = new ArrayList<EntityImpl>();
    newDoc.field("embeddedList", list, PropertyType.EMBEDDEDLIST);
    list.add(((EntityImpl) session.newEntity()).field("name", "Luca"));
    list.add(((EntityImpl) session.newEntity("Account")).field("name", "Marcus"));

    session.begin();
    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<EntityImpl>) loadedDoc.field("embeddedList")).getFirst() instanceof EntityImpl);

    var d = ((List<EntityImpl>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<EntityImpl>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(d.getSchemaClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testLinkList() {
    var newDoc = ((EntityImpl) session.newEntity());

    final var list = new ArrayList<EntityImpl>();
    newDoc.field("linkedList", list, PropertyType.LINKLIST);
    session.begin();

    var doc = ((EntityImpl) session.newEntity());
    doc.field("name", "Luca")
        .save();
    list.add(doc);

    list.add(((EntityImpl) session.newEntity("Account")).field("name", "Marcus"));

    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedList"));
    Assert.assertTrue(loadedDoc.field("linkedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<Identifiable>) loadedDoc.field("linkedList")).getFirst() instanceof Identifiable);

    EntityImpl d = ((List<Identifiable>) loadedDoc.field("linkedList")).getFirst().getRecord(
        session);
    Assert.assertTrue(d.getIdentity().isValid());
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<Identifiable>) loadedDoc.field("linkedList")).get(1).getRecord(session);
    Assert.assertEquals(d.getSchemaClassName(), "Account");
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testEmbeddedSet() {
    var newDoc = ((EntityImpl) session.newEntity());

    final Set<EntityImpl> set = new HashSet<>();
    newDoc.field("embeddedSet", set, PropertyType.EMBEDDEDSET);
    set.add(((EntityImpl) session.newEntity()).field("name", "Luca"));
    set.add(((EntityImpl) session.newEntity("Account")).field("name", "Marcus"));

    session.begin();
    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedSet"));
    Assert.assertNotNull(loadedDoc.getEmbeddedSet("embeddedSet"));

    final var it =
        ((Collection<EntityImpl>) loadedDoc.field("embeddedSet")).iterator();

    var tot = 0;
    while (it.hasNext()) {
      var d = it.next();
      Assert.assertTrue(d instanceof EntityImpl);

      if (d.field("name").equals("Marcus")) {
        Assert.assertEquals(d.getSchemaClassName(), "Account");
      }

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testLinkSet() {
    var newDoc = ((EntityImpl) session.newEntity());

    final Set<EntityImpl> set = new HashSet<>();
    newDoc.field("linkedSet", set, PropertyType.LINKSET);
    session.begin();
    var doc = ((EntityImpl) session.newEntity());
    doc.field("name", "Luca")
        .save();
    set.add(doc);

    set.add(((EntityImpl) session.newEntity("Account")).field("name", "Marcus"));

    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertTrue(loadedDoc.containsField("linkedSet"));
    Assert.assertNotNull(loadedDoc.getLinkSet("linkedSet"));

    final var it =
        ((Collection<Identifiable>) loadedDoc.field("linkedSet")).iterator();

    var tot = 0;
    while (it.hasNext()) {
      var d = it.next().getEntity(session);

      if (Objects.equals(d.getProperty("name"), "Marcus")) {
        Assert.assertEquals(d.getSchemaClassName(), "Account");
      }

      ++tot;
    }

    Assert.assertEquals(tot, 2);
  }

  @Test
  public void testEmbeddedMap() {
    var newDoc = ((EntityImpl) session.newEntity());

    final Map<String, EntityImpl> map = new HashMap<>();
    newDoc.field("embeddedMap", map, PropertyType.EMBEDDEDMAP);
    map.put("Luca", ((EntityImpl) session.newEntity()).field("name", "Luca"));
    map.put("Marcus", ((EntityImpl) session.newEntity()).field("name", "Marcus"));
    map.put("Cesare", ((EntityImpl) session.newEntity("Account")).field("name", "Cesare"));

    session.begin();
    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, EntityImpl>) loadedDoc.field("embeddedMap")).values().iterator().next()
            instanceof EntityImpl);

    var d = ((Map<String, EntityImpl>) loadedDoc.field("embeddedMap")).get("Luca");
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, EntityImpl>) loadedDoc.field("embeddedMap")).get("Marcus");
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, EntityImpl>) loadedDoc.field("embeddedMap")).get("Cesare");
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getSchemaClassName(), "Account");
  }

  @Test
  public void testEmptyEmbeddedMap() {
    var newDoc = ((EntityImpl) session.newEntity());

    final Map<String, EntityImpl> map = new HashMap<>();
    newDoc.field("embeddedMap", map, PropertyType.EMBEDDEDMAP);

    session.begin();
    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);

    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, EntityImpl> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testLinkMap() {
    var newDoc = ((EntityImpl) session.newEntity());

    final Map<String, EntityImpl> map = new HashMap<>();
    newDoc.field("linkedMap", map, PropertyType.LINKMAP);
    session.begin();
    var doc1 = ((EntityImpl) session.newEntity());
    doc1.field("name", "Luca")
        .save();
    map.put("Luca", doc1);
    var doc2 = ((EntityImpl) session.newEntity());
    doc2.field("name", "Marcus")
        .save();
    map.put("Marcus", doc2);

    var doc3 = ((EntityImpl) session.newEntity("Account"));
    doc3.field("name", "Cesare").save();
    map.put("Cesare", doc3);

    session.save(newDoc);
    session.commit();

    final RID rid = newDoc.getIdentity();

    session.close();
    session = acquireSession();

    EntityImpl loadedDoc = session.load(rid);
    Assert.assertNotNull(loadedDoc.getLinkMap("linkedMap"));
    Assert.assertTrue(loadedDoc.field("linkedMap") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, Identifiable>) loadedDoc.field("linkedMap")).values().iterator().next()
            instanceof Identifiable);

    EntityImpl d =
        ((Map<String, Identifiable>) loadedDoc.field("linkedMap")).get("Luca").getRecord(session);
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, Identifiable>) loadedDoc.field("linkedMap")).get("Marcus").getRecord(session);
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, Identifiable>) loadedDoc.field("linkedMap")).get("Cesare").getRecord(session);
    Assert.assertEquals(d.field("name"), "Cesare");
    Assert.assertEquals(d.getSchemaClassName(), "Account");
  }
}
