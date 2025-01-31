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

import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test
public class SQLSelectTestNewTest extends AbstractSelectTest {

  private EntityImpl record = ((EntityImpl) db.newEntity());

  @Parameters(value = "remote")
  public SQLSelectTestNewTest(boolean remote) throws Exception {
    super(remote);
  }

  @BeforeClass
  public void init() {
    if (!db.getMetadata().getSchema().existsClass("Profile")) {
      db.getMetadata().getSchema().createClass("Profile", 1);

      for (var i = 0; i < 1000; ++i) {
        db.begin();
        db.<EntityImpl>newInstance("Profile").field("test", i).field("name", "N" + i)
            .save();
        db.commit();
      }
    }

    if (!db.getMetadata().getSchema().existsClass("company")) {
      db.getMetadata().getSchema().createClass("company", 1);
      for (var i = 0; i < 20; ++i) {
        db.begin();
        ((EntityImpl) db.newEntity("company")).field("id", i).save();
        db.commit();
      }
    }

    db.getMetadata().getSchema().getOrCreateClass("Account");
  }

  @Test
  public void queryNoDirtyResultset() {
    var result = executeQuery(" select from Profile ", db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void queryNoWhere() {
    var result = executeQuery(" select from Profile ", db);
    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void queryParentesisAsRight() {
    var result =
        executeQuery(
            "  select from Profile where ( name = 'Giuseppe' and ( name <> 'Napoleone' and nick is"
                + " not null ))  ",
            db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void querySingleAndDoubleQuotes() {
    var result = executeQuery("select from Profile where name = 'Giuseppe'",
        db);

    final var count = result.size();
    Assert.assertFalse(result.isEmpty());

    result = executeQuery("select from Profile where name = \"Giuseppe\"", db);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.size(), count);
  }

  @Test
  public void queryTwoParentesisConditions() {
    var result =
        executeQuery(
            "select from Profile  where ( name = 'Giuseppe' and nick is not null ) or ( name ="
                + " 'Napoleone' and nick is not null ) ",
            db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void testQueryCount() {
    db.getMetadata().reload();
    final var vertexesCount = db.countClass("V");
    var result =
        db.query("select count(*) as count from V").toList();
    Assert.assertEquals(result.getFirst().<Object>getProperty("count"), vertexesCount);
  }

  @Test
  public void querySchemaAndLike() {
    var result1 =
        executeQuery("select * from cluster:profile where name like 'Gi%'", db);

    for (var value : result1) {
      record = (EntityImpl) value.asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().startsWith("Gi"));
    }

    var result2 =
        executeQuery("select * from cluster:profile where name like '%epp%'", db);

    Assert.assertEquals(result1, result2);

    var result3 =
        executeQuery("select * from cluster:profile where name like 'Gius%pe'", db);

    Assert.assertEquals(result1, result3);

    result1 = executeQuery("select * from cluster:profile where name like '%Gi%'", db);

    for (var result : result1) {
      record = (EntityImpl) result.asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }

    result1 = executeQuery("select * from cluster:profile where name like ?", db, "%Gi%");

    for (var result : result1) {
      record = (EntityImpl) result.asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }
  }

  @Test
  public void queryContainsInEmbeddedSet() {
    Set<String> tags = new HashSet<String>();
    tags.add("smart");
    tags.add("nice");

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("tags", tags, PropertyType.EMBEDDEDSET);

    doc.save();
    db.commit();

    var resultset =
        executeQuery("select from Profile where tags CONTAINS 'smart'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedList() {
    List<String> tags = new ArrayList<String>();
    tags.add("smart");
    tags.add("nice");

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("tags", tags);

    doc.save();
    db.commit();

    var resultset =
        executeQuery("select from Profile where tags[0] = 'smart'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    resultset =
        executeQuery("select from Profile where tags[0,1] CONTAINSALL ['smart','nice']", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentSet() {
    var coll = new HashSet<EntityImpl>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("coll", coll, PropertyType.EMBEDDEDSET);

    doc.save();
    db.commit();

    var resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertTrue(resultset.getFirst().getProperty("value") instanceof Collection);

    Collection<?> xcoll = resultset.getFirst().getProperty("value");
    Assert.assertEquals(((Entity) xcoll.iterator().next()).getProperty("name"), "Jay");

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentList() {
    var coll = new ArrayList<>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("coll", coll, PropertyType.EMBEDDEDLIST);

    doc.save();
    db.commit();

    var resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    //    Assert.assertEquals(resultset.get(0).field("value").getClass(), EntityImpl.class);
    var result = resultset.getFirst().getProperty("value");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(((Collection<?>) result).size(), 1);
    Assert.assertEquals(
        ((Entity) ((Collection<?>) result).iterator().next()).getProperty("name"), "Jay");

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapClassic() {
    Map<String, EntityImpl> customReferences = new HashMap<>();
    customReferences.put("first", new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    doc.save();
    db.commit();

    var resultset =
        executeQuery("select from Profile where customReferences CONTAINSKEY 'first'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences CONTAINSVALUE ( name like 'Ja%')",
            db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences['second']['name'] like 'Ja%'", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());
    resultset =
        executeQuery(
            "select customReferences['second', 'first'] as customReferences from Profile where"
                + " customReferences.size() = 2",
            db);
    Assert.assertEquals(resultset.size(), 1);

    if (resultset.getFirst().getProperty("customReferences").getClass().isArray()) {
      Object[] customReferencesBack = resultset.getFirst().getProperty("customReferences");
      Assert.assertEquals(customReferencesBack.length, 2);
      Assert.assertTrue(customReferencesBack[0] instanceof EntityImpl);
      Assert.assertTrue(customReferencesBack[1] instanceof EntityImpl);
    } else if (resultset.getFirst().getProperty("customReferences") instanceof List) {
      List<EntityImpl> customReferencesBack = resultset.getFirst().getProperty("customReferences");
      Assert.assertEquals(customReferencesBack.size(), 2);
      Assert.assertTrue(customReferencesBack.get(0) instanceof EntityImpl);
      Assert.assertTrue(customReferencesBack.get(1) instanceof EntityImpl);
    } else {
      Assert.fail("Wrong type received: " + resultset.getFirst().getProperty("customReferences"));
    }

    resultset =
        executeQuery(
            "select customReferences['second']['name'] from Profile where"
                + " customReferences['second']['name'] is not null",
            db);
    Assert.assertEquals(resultset.size(), 1);

    resultset =
        executeQuery(
            "select customReferences['second']['name'] as value from Profile where"
                + " customReferences['second']['name'] is not null",
            db);
    Assert.assertEquals(resultset.size(), 1);

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapNew() {
    db.begin();
    Map<String, EntityImpl> customReferences = new HashMap<>();
    customReferences.put("first", new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    doc.save();
    db.commit();

    var resultset =
        executeQuery(
            "select from Profile where customReferences.keys() CONTAINS 'first'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences.values() CONTAINS ( name like 'Ja%')",
            db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
    var result =
        executeQuery(
            "select * from cluster:profile where races contains"
                + " (name.toLowerCase(Locale.ENGLISH).subString(0,1) = 'e')",
            db);

    for (var value : result) {
      record = (EntityImpl) value.asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertNotNull(record.field("races"));

      Collection<EntityImpl> races = record.field("races");
      var found = false;
      for (var race : races) {
        if (((String) race.field("name")).toLowerCase(Locale.ENGLISH).charAt(0) == 'e') {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  @Test
  public void queryCollectionContainsInRecords() {
    record = ((EntityImpl) db.newEntity("Animal"));
    record.field("name", "Cat");

    db.begin();
    Collection<EntityImpl> races = new HashSet<EntityImpl>();
    races.add(((EntityImpl) db.newInstance("AnimalRace")).field("name", "European"));
    races.add(((EntityImpl) db.newInstance("AnimalRace")).field("name", "Siamese"));
    record.field("age", 10);
    record.field("races", races);
    record.save();
    db.commit();

    var result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['European','Asiatic'])",
            db);

    var found = false;
    for (var i = 0; i < result.size() && !found; ++i) {
      record = (EntityImpl) result.get(i).asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (var race : races) {
        if (race.field("name").equals("European") || race.field("name").equals("Asiatic")) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['Asiatic','European'])",
            db);

    found = false;
    for (var i = 0; i < result.size() && !found; ++i) {
      record = (EntityImpl) result.get(i).asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (var race : races) {
        if (race.field("name").equals("European") || race.field("name").equals("Asiatic")) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['aaa','bbb'])", db);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (name in ['European','Asiatic'])",
            db);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (name in ['European','Siamese'])",
            db);
    Assert.assertEquals(result.size(), 1);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (age < 100) LIMIT 1000 SKIP 0",
            db);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where not ( races contains (age < 100) ) LIMIT 20 SKIP 0",
            db);
    Assert.assertEquals(result.size(), 1);

    db.begin();
    record.delete();
    db.commit();
  }

  @Test
  public void queryCollectionInNumbers() {
    record = ((EntityImpl) db.newEntity("Animal"));
    record.field("name", "Cat");

    Collection<Integer> rates = new HashSet<Integer>();
    rates.add(100);
    rates.add(200);
    record.field("rates", rates);

    db.begin();
    record.save("animal");
    db.commit();

    var result =
        executeQuery("select * from cluster:animal where rates contains 500", db);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where rates contains 100", db);
    Assert.assertEquals(result.size(), 1);

    db.begin();
    record.delete();
    db.commit();
  }

  @Test
  public void queryWhereRidDirectMatching() {
    var clusterId = db.getMetadata().getSchema().getClass("ORole").getClusterIds()[0];
    var positions = getValidPositions(clusterId);

    var result =
        executeQuery(
            "select * from OUser where roles contains #" + clusterId + ":" + positions.getFirst(),
            db);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryWhereInpreparred() {
    var result =
        executeQuery("select * from OUser where name in [ :name ]", db, "admin");

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((EntityImpl) result.getFirst().asEntity()).field("name"), "admin");
  }

  @Test
  public void queryAllOperator() {
    var result = executeQuery("select from Account where all() is null", db);

    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void queryOrderBy() {
    var result = executeQuery("select from Profile order by name", db);

    Assert.assertFalse(result.isEmpty());

    String lastName = null;
    var isNullSegment = true; // NULL VALUES AT THE BEGINNING!
    for (var d : result) {
      final String fieldValue = d.getProperty("name");
      if (fieldValue != null) {
        isNullSegment = false;
      } else {
        Assert.assertTrue(isNullSegment);
      }

      if (lastName != null) {
        Assert.assertTrue(fieldValue.compareTo(lastName) >= 0);
      }
      lastName = fieldValue;
    }
  }

  @Test
  public void queryOrderByWrongSyntax() {
    try {
      executeQuery("select from Profile order by name aaaa", db);
      Assert.fail();
    } catch (CommandSQLParsingException ignored) {
    }
  }

  @Test
  public void queryLimitOnly() {
    var result = executeQuery("select from Profile limit 1", db);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void querySkipOnly() {
    var result = executeQuery("select from Profile", db);
    var total = result.size();

    result = executeQuery("select from Profile skip 1", db);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithSkipAndLimit() {
    var result = executeQuery("select from Profile", db);

    var page = executeQuery("select from Profile skip 10 limit 10", db);
    Assert.assertEquals(page.size(), 10);

    for (var i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), result.get(10 + i));
    }
  }

  @Test
  public void queryOffsetOnly() {
    var result = executeQuery("select from Profile", db);
    var total = result.size();

    result = executeQuery("select from Profile offset 1", db);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithOffsetAndLimit() {
    var result = executeQuery("select from Profile", db);

    var page = executeQuery("select from Profile offset 10 limit 10", db);
    Assert.assertEquals(page.size(), 10);

    for (var i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderBySkipAndLimit() {
    var result = executeQuery("select from Profile order by name", db);

    var page =
        executeQuery("select from Profile order by name limit 10 skip 10", db);
    Assert.assertEquals(page.size(), 10);

    for (var i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderByDescSkipAndLimit() {
    var result = executeQuery("select from Profile order by name desc", db);

    var page =
        executeQuery("select from Profile order by name desc limit 10 skip 10", db);
    Assert.assertEquals(page.size(), 10);

    for (var i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), result.get(10 + i));
    }
  }

  @Test
  public void queryOrderByAndLimit() {
    var result = executeQuery("select from Profile order by name limit 2", db);

    Assert.assertTrue(result.size() <= 2);

    String lastName = null;
    for (var d : result) {
      if (lastName != null && d.getProperty("name") != null) {
        Assert.assertTrue(((String) d.getProperty("name")).compareTo(lastName) >= 0);
      }
      lastName = d.getProperty("name");
    }
  }

  @Test
  public void queryConditionAndOrderBy() {
    var result =
        executeQuery("select from Profile where name is not null order by name", db);

    Assert.assertFalse(result.isEmpty());

    String lastName = null;
    for (var d : result) {
      if (lastName != null && d.getProperty("name") != null) {
        Assert.assertTrue(((String) d.getProperty("name")).compareTo(lastName) >= 0);
      }
      lastName = d.getProperty("name");
    }
  }

  @Test
  public void queryConditionsAndOrderBy() {
    var result =
        executeQuery(
            "select from Profile where name is not null order by name desc, id asc", db);

    Assert.assertFalse(result.isEmpty());

    String lastName = null;
    for (var d : result) {
      if (lastName != null && d.getProperty("name") != null) {
        Assert.assertTrue(((String) d.getProperty("name")).compareTo(lastName) <= 0);
      }
      lastName = d.getProperty("name");
    }
  }

  @Test
  public void queryRecordTargetRid() {
    var profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    var positions = getValidPositions(profileClusterId);

    var result =
        executeQuery("select from " + profileClusterId + ":" + positions.getFirst(), db);

    Assert.assertEquals(result.size(), 1);

    for (var d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + positions.getFirst());
    }
  }

  @Test
  public void queryRecordTargetRids() {
    var profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    var positions = getValidPositions(profileClusterId);

    var result =
        executeQuery(
            " select from ["
                + profileClusterId
                + ":"
                + positions.get(0)
                + ", "
                + profileClusterId
                + ":"
                + positions.get(1)
                + "]",
            db);

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(
        result.get(0).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    Assert.assertEquals(
        result.get(1).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(1));
  }

  @Test
  public void queryRecordAttribRid() {

    var profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    var postions = getValidPositions(profileClusterId);

    var result =
        executeQuery(
            "select from Profile where @rid = #" + profileClusterId + ":" + postions.getFirst(),
            db);

    Assert.assertEquals(result.size(), 1);

    for (var d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + postions.getFirst());
    }
  }

  @Test
  public void queryRecordAttribClass() {
    var result = executeQuery("select from Profile where @class = 'Profile'",
        db);

    Assert.assertFalse(result.isEmpty());

    for (var d : result) {
      Assert.assertEquals(d.asEntity().getClassName(), "Profile");
    }
  }

  @Test
  public void queryRecordAttribVersion() {
    var result = executeQuery("select from Profile where @version > 0", db);

    Assert.assertFalse(result.isEmpty());

    for (var d : result) {
      Assert.assertTrue(d.asEntity().getVersion() > 0);
    }
  }

  @Test
  public void queryRecordAttribType() {
    var result = executeQuery("select from Profile where @type = 'document'",
        db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void queryWrongOperator() {
    try {
      executeQuery("select from Profile where name like.toLowerCase() '%Jay%'", db);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void queryEscaping() {
    executeQuery("select from Profile where name like '%\\'Jay%'", db);
  }

  @Test
  public void queryWithLimit() {
    Assert.assertEquals(executeQuery("select from Profile limit 3", db).size(), 3);
  }

  @SuppressWarnings("unused")
  @Test
  public void testRecordNumbers() {
    var tot = db.countClass("V");

    var count = 0;
    for (var record : db.browseClass("V")) {
      count++;
    }

    Assert.assertEquals(count, tot);

    Assert.assertTrue(executeQuery("select from V", db).size() >= tot);
  }

  @Test
  public void queryWithManualPagination() {
    RID last = new ChangeableRecordId();
    var resultset =
        executeQuery("select from Profile where @rid > ? LIMIT 3", db, last);

    var iterationCount = 0;
    Assert.assertFalse(resultset.isEmpty());
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().orElseThrow().getClusterId() < 0
                || (d.getIdentity().orElseThrow().getClusterId() >= last.getClusterId())
                && d.getIdentity().orElseThrow().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity().orElseThrow();

      iterationCount++;
      resultset = executeQuery("select from Profile where @rid > ? LIMIT 3", db, last);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPagination() {
    final var query =
        new SQLSynchQuery<EntityImpl>("select from Profile LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query);

    var iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity();

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationAndRidInWhere() {
    var clusterId = db.getClusterIdByName("profile");

    var range = db.getStorage().getClusterDataRange(db, clusterId);

    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where @rid between #"
                + clusterId
                + ":"
                + range[0]
                + " and #"
                + clusterId
                + ":"
                + range[1]
                + " LIMIT 3");

    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query);

    Assert.assertEquals(resultset.getFirst().getIdentity(), new RecordId(clusterId, range[0]));

    var iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity();

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertEquals(last, new RecordId(clusterId, range[1]));
    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhere() {
    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > 0 LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query);

    var iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity();

      // System.out.printf("\nIterating page %d, last record is %s", iterationCount, last);

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVar() {
    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query, 0);

    var iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity();

      iterationCount++;
      resultset = db.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVarAtTheFirstQueryCall() {
    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query, 0);

    var iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (var d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.getLast().getIdentity();

      iterationCount++;
      resultset = db.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAbsenceOfAutomaticPaginationBecauseOfBindingVarReset() {
    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");

    List<EntityImpl> resultset = db.query(query, -1);

    final RID firstRidFirstQuery = resultset.getFirst().getIdentity();

    resultset = db.query(query, -2);

    final RID firstRidSecondQueryQuery = resultset.getFirst().getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void includeFields() {
    final var query =
        new SQLSynchQuery<EntityImpl>("select expand( roles.include('name') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (var d : resultset) {
      Assert.assertTrue(d.fields() <= 1);
      if (d.fields() == 1) {
        Assert.assertTrue(d.containsField("name"));
      }
    }
  }

  @Test
  public void excludeFields() {
    final var query =
        new SQLSynchQuery<EntityImpl>("select expand( roles.exclude('rules') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (var d : resultset) {
      Assert.assertFalse(d.containsField("rules"));
    }
  }

  @Test
  public void excludeAttributes() {
    final var query =
        new SQLSynchQuery<EntityImpl>(
            "select expand( roles.exclude('@rid', '@class') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (var d : resultset) {
      Assert.assertFalse(d.getIdentity().isPersistent());
      Assert.assertNull(d.getSchemaClass());
    }
  }

  @Test
  public void queryResetPagination() {
    final var query =
        new SQLSynchQuery<EntityImpl>("select from Profile LIMIT 3");

    List<EntityImpl> resultset = db.query(query);
    final RID firstRidFirstQuery = resultset.getFirst().getIdentity();
    query.resetPagination();

    resultset = db.query(query);
    final RID firstRidSecondQueryQuery = resultset.getFirst().getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void queryBetween() {
    var result =
        executeQuery("select * from account where nr between 10 and 20", db);

    for (var value : result) {
      record = (EntityImpl) value.asEntity();

      Assert.assertTrue(
          ((Integer) record.field("nr")) >= 10 && ((Integer) record.field("nr")) <= 20);
    }
  }

  @Test
  public void queryParenthesisInStrings() {

    db.command("INSERT INTO account (name) VALUES ('test (demo)')").close();

    var result =
        executeQuery("select * from account where name = 'test (demo)'", db);

    Assert.assertEquals(result.size(), 1);

    for (var value : result) {
      record = (EntityImpl) value.asEntity();
      Assert.assertEquals(record.field("name"), "test (demo)");
    }
  }

  @Test
  public void queryMathOperators() {
    var result = executeQuery("select * from account where id < 3 + 4", db);
    Assert.assertFalse(result.isEmpty());
    for (var result3 : result) {
      Assert.assertTrue(((Number) result3.getProperty("id")).intValue() < 3 + 4);
    }

    result = executeQuery("select * from account where id < 10 - 3", db);
    Assert.assertFalse(result.isEmpty());
    for (var result1 : result) {
      Assert.assertTrue(((Number) result1.getProperty("id")).intValue() < 10 - 3);
    }

    result = executeQuery("select * from account where id < 3 * 2", db);
    Assert.assertFalse(result.isEmpty());
    for (var element : result) {
      Assert.assertTrue(((Number) element.getProperty("id")).intValue() < 3 << 1);
    }

    result = executeQuery("select * from account where id < 120 / 20", db);
    Assert.assertFalse(result.isEmpty());
    for (var item : result) {
      Assert.assertTrue(((Number) item.getProperty("id")).intValue() < 120 / 20);
    }

    result = executeQuery("select * from account where id < 27 % 10", db);
    Assert.assertFalse(result.isEmpty());
    for (var value : result) {
      Assert.assertTrue(((Number) value.getProperty("id")).intValue() < 27 % 10);
    }

    result = executeQuery("select * from account where id = id * 1", db);
    Assert.assertFalse(result.isEmpty());

    var result2 =
        executeQuery("select count(*) as tot from account where id >= 0", db);
    Assert.assertEquals(result.size(), ((Number) result2.getFirst().getProperty("tot")).intValue());
  }

  @Test
  public void testBetweenWithParameters() {

    final var result =
        executeQuery(
            "select * from company where id between ? and ? and salary is not null",
            db,
            4,
            7);

    System.out.println("testBetweenWithParameters:");
    for (var d : result) {
      System.out.println(d);
    }

    Assert.assertEquals(result.size(), 4, "Found: " + result);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (var record : result) {
      Assert.assertTrue(resultsList.remove(record.<Integer>getProperty("id")));
    }
  }

  @Test
  public void testInWithParameters() {

    final var result =
        executeQuery(
            "select * from company where id in [?, ?, ?, ?] and salary is not null",
            db,
            4,
            5,
            6,
            7);

    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (var record : result) {
      Assert.assertTrue(resultsList.remove(record.<Integer>getProperty("id")));
    }
  }

  @Test
  public void testEqualsNamedParameter() {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final var result =
        executeQuery(
            "select * from company where id = :id and salary is not null", db, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testQueryAsClass() {

    var result =
        executeQuery("select from Account where addresses.@class in [ 'Address' ]", db);
    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("addresses"));
      Assert.assertEquals(
          ((EntityImpl)
              (d.<Collection<Identifiable>>getProperty("addresses")).iterator().next()
                  .getRecord(db))
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  @Test
  public void testQueryNotOperator() {

    var result =
        executeQuery(
            "select from Account where not ( addresses.@class in [ 'Address' ] )", db);
    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertTrue(
          d.getProperty("addresses") == null
              || (d.<Collection<Identifiable>>getProperty("addresses")).isEmpty()
              || !((EntityImpl)
              (d.<Collection<Identifiable>>getProperty("addresses"))
                  .iterator()
                  .next()
                  .getRecord(db))
              .getSchemaClass()
              .getName()
              .equals("Address"));
    }
  }

  @Test
  public void testSquareBracketsOnCondition() {
    var result =
        executeQuery(
            "select from Account where addresses[@class='Address'][city.country.name] ="
                + " 'Washington'",
            db);
    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("addresses"));
      Assert.assertEquals(
          ((EntityImpl)
              (d.<Collection<Identifiable>>getProperty("addresses")).iterator().next()
                  .getRecord(db))
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  public void testParams() {
    var test = db.getMetadata().getSchema().getClass("test");
    if (test == null) {
      test = db.getMetadata().getSchema().createClass("test");
      test.createProperty(db, "f1", PropertyType.STRING);
      test.createProperty(db, "f2", PropertyType.STRING);
    }
    var document = ((EntityImpl) db.newEntity(test));
    document.field("f1", "a").field("f2", "a");

    db.begin();
    db.save(document);
    db.commit();

    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("p1", "a");
    db.query(new SQLSynchQuery<EntityImpl>("select from test where (f1 = :p1)"),
        parameters);
    db.query(
        new SQLSynchQuery<EntityImpl>("select from test where f1 = :p1 and f2 = :p1"),
        parameters);
  }

  @Test
  public void queryInstanceOfOperator() {
    var result = executeQuery("select from Account", db);

    Assert.assertFalse(result.isEmpty());

    var result2 =
        executeQuery("select from Account where @this instanceof 'Account'", db);

    Assert.assertEquals(result2.size(), result.size());

    var result3 =
        executeQuery("select from Account where @class instanceof 'Account'", db);

    Assert.assertEquals(result3.size(), result.size());
  }

  @Test
  public void subQuery() {
    var result =
        executeQuery(
            "select from Account where name in ( select name from Account where name is not null"
                + " limit 1 )",
            db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void subQueryNoFrom() {
    var result2 =
        executeQuery(
            "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
                + " addresses.size() > 0 )",
            db);

    Assert.assertFalse(result2.isEmpty());
    Assert.assertTrue(result2.getFirst().getProperty("$names") instanceof Collection<?>);
    Assert.assertFalse(((Collection<?>) result2.getFirst().getProperty("$names")).isEmpty());
  }

  @Test
  public void subQueryLetAndIndexedWhere() {
    var result =
        executeQuery("select $now from OUser let $now = eval('42') where name = 'admin'", db);

    Assert.assertEquals(result.size(), 1);
    Assert.assertNotNull(result.getFirst().getProperty("$now"), result.getFirst().toString());
  }

  @Test
  public void queryOrderByWithLimit() {

    Schema schema = db.getMetadata().getSchema();
    var facClass = schema.getClass("FicheAppelCDI");
    if (facClass == null) {
      facClass = schema.createClass("FicheAppelCDI");
    }
    if (!facClass.existsProperty("date")) {
      facClass.createProperty(db, "date", PropertyType.DATE);
    }

    final var currentYear = Calendar.getInstance();
    final var oneYearAgo = Calendar.getInstance();
    oneYearAgo.add(Calendar.YEAR, -1);

    db.begin();
    var doc1 = ((EntityImpl) db.newEntity(facClass));
    doc1.field("context", "test");
    doc1.field("date", currentYear.getTime());
    doc1.save();

    var doc2 = ((EntityImpl) db.newEntity(facClass));
    doc2.field("context", "test");
    doc2.field("date", oneYearAgo.getTime());
    doc2.save();
    db.commit();

    var result =
        db.query("select * from " + facClass.getName() + " where context = 'test' order by date",
            1).toList();

    var smaller = Calendar.getInstance();
    smaller.setTime(result.getFirst().getProperty("date"));
    Assert.assertEquals(smaller.get(Calendar.YEAR), oneYearAgo.get(Calendar.YEAR));

    result =
        db.query(
            "select * from "
                + facClass.getName()
                + " where context = 'test' order by date DESC",
            1).toList();

    var bigger = Calendar.getInstance();
    bigger.setTime(result.getFirst().getProperty("date"));
    Assert.assertEquals(bigger.get(Calendar.YEAR), currentYear.get(Calendar.YEAR));
  }

  @Test
  public void queryWithTwoRidInWhere() {
    var clusterId = db.getClusterIdByName("profile");

    var positions = getValidPositions(clusterId);

    final long minPos;
    final long maxPos;
    if (positions.get(5).compareTo(positions.get(25)) > 0) {
      minPos = positions.get(25);
      maxPos = positions.get(5);
    } else {
      minPos = positions.get(5);
      maxPos = positions.get(25);
    }

    var resultset =
        executeQuery(
            "select @rid.trim() as oid, name from Profile where (@rid in [#"
                + clusterId
                + ":"
                + positions.get(5)
                + "] or @rid in [#"
                + clusterId
                + ":"
                + positions.get(25)
                + "]) AND @rid > ? LIMIT 10000",
            db,
            new RecordId(clusterId, minPos));

    Assert.assertEquals(resultset.size(), 1);

    Assert.assertEquals(resultset.getFirst().getProperty("oid"),
        new RecordId(clusterId, maxPos).toString());
  }

  @Test
  public void testSelectFromListParameter() {
    var placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(db, "id", PropertyType.STRING);
    placeClass.createProperty(db, "descr", PropertyType.STRING);
    placeClass.createIndex(db, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    var odoc = ((EntityImpl) db.newEntity("Place"));
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");

    db.begin();
    db.save(odoc);
    db.commit();

    odoc = ((EntityImpl) db.newEntity("Place"));
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");

    db.begin();
    db.save(odoc);
    db.commit();

    Map<String, Object> params = new HashMap<String, Object>();
    List<String> inputValues = new ArrayList<String>();
    inputValues.add("lago_di_como");
    inputValues.add("lecco");
    params.put("place", inputValues);

    var result = executeQuery("select from place where id in :place", db,
        params);
    Assert.assertEquals(result.size(), 1);

    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidFromListParameter() {
    var placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(db, "id", PropertyType.STRING);
    placeClass.createProperty(db, "descr", PropertyType.STRING);
    placeClass.createIndex(db, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    List<RID> inputValues = new ArrayList<RID>();

    var odoc = ((EntityImpl) db.newEntity("Place"));
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");

    db.begin();
    db.save(odoc);
    db.commit();

    inputValues.add(odoc.getIdentity());

    odoc = ((EntityImpl) db.newEntity("Place"));
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");

    db.begin();
    db.save(odoc);
    db.commit();
    inputValues.add(odoc.getIdentity());

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("place", inputValues);

    var result =
        executeQuery("select from place where @rid in :place", db, params);
    Assert.assertEquals(result.size(), 2);

    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidInList() {
    var placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    db.getMetadata().getSchema().createClass("FamousPlace", 1, placeClass);

    var firstPlace = ((EntityImpl) db.newEntity("Place"));

    db.begin();
    db.save(firstPlace);
    var secondPlace = ((EntityImpl) db.newEntity("Place"));
    db.save(secondPlace);
    var famousPlace = ((EntityImpl) db.newEntity("FamousPlace"));
    db.save(famousPlace);
    db.commit();

    RID secondPlaceId = secondPlace.getIdentity();
    RID famousPlaceId = famousPlace.getIdentity();
    // if one of these two asserts fails, the test will be meaningless.
    Assert.assertTrue(secondPlaceId.getClusterId() < famousPlaceId.getClusterId());
    Assert.assertTrue(secondPlaceId.getClusterPosition() > famousPlaceId.getClusterPosition());

    var result =
        executeQuery(
            "select from Place where @rid in [" + secondPlaceId + "," + famousPlaceId + "]",
            db);
    Assert.assertEquals(result.size(), 2);

    db.getMetadata().getSchema().dropClass("FamousPlace");
    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testMapKeys() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final var result =
        executeQuery(
            "select * from company where id = :id and salary is not null", db, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAsynch() {
    final var sqlOne = "select * from company where id between 4 and 7";
    final var sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<EntityImpl> synchResultOne =
        db.command(new SQLSynchQuery<EntityImpl>(sqlOne)).execute(db);
    final List<EntityImpl> synchResultTwo =
        db.command(new SQLSynchQuery<EntityImpl>(sqlTwo)).execute(db);

    Assert.assertFalse(synchResultOne.isEmpty());
    Assert.assertFalse(synchResultTwo.isEmpty());

    final List<EntityImpl> asynchResultOne = new ArrayList<EntityImpl>();
    final List<EntityImpl> asynchResultTwo = new ArrayList<EntityImpl>();
    final var endOneCalled = new AtomicBoolean();
    final var endTwoCalled = new AtomicBoolean();

    db
        .command(
            new SQLAsynchQuery<EntityImpl>(
                sqlOne,
                new CommandResultListener() {
                  @Override
                  public boolean result(DatabaseSessionInternal db, Object iRecord) {
                    asynchResultOne.add((EntityImpl) iRecord);
                    return true;
                  }

                  @Override
                  public void end() {
                    endOneCalled.set(true);

                    db
                        .command(
                            new SQLAsynchQuery<EntityImpl>(
                                sqlTwo,
                                new CommandResultListener() {
                                  @Override
                                  public boolean result(DatabaseSessionInternal db,
                                      Object iRecord) {
                                    asynchResultTwo.add((EntityImpl) iRecord);
                                    return true;
                                  }

                                  @Override
                                  public void end() {
                                    endTwoCalled.set(true);
                                  }

                                  @Override
                                  public Object getResult() {
                                    return null;
                                  }
                                }))
                        .execute(db);
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }))
        .execute(db);

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(
        EntityHelper.compareCollections(
            db, synchResultTwo, db, asynchResultTwo, null),
        "synchResultTwo=" + synchResultTwo.size() + " asynchResultTwo=" + asynchResultTwo.size());
    Assert.assertTrue(
        EntityHelper.compareCollections(
            db, synchResultOne, db, asynchResultOne, null),
        "synchResultOne=" + synchResultOne.size() + " asynchResultOne=" + asynchResultOne.size());
  }

  @Test
  public void queryAsynchHalfForheFirstQuery() {
    final var sqlOne = "select * from company where id between 4 and 7";
    final var sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<EntityImpl> synchResultOne =
        db.command(new SQLSynchQuery<EntityImpl>(sqlOne)).execute(db);
    final List<EntityImpl> synchResultTwo =
        db.command(new SQLSynchQuery<EntityImpl>(sqlTwo)).execute(db);

    Assert.assertFalse(synchResultOne.isEmpty());
    Assert.assertFalse(synchResultTwo.isEmpty());

    final List<EntityImpl> asynchResultOne = new ArrayList<EntityImpl>();
    final List<EntityImpl> asynchResultTwo = new ArrayList<EntityImpl>();
    final var endOneCalled = new AtomicBoolean();
    final var endTwoCalled = new AtomicBoolean();

    db
        .command(
            new SQLAsynchQuery<EntityImpl>(
                sqlOne,
                new CommandResultListener() {
                  @Override
                  public boolean result(DatabaseSessionInternal db, Object iRecord) {
                    asynchResultOne.add((EntityImpl) iRecord);
                    return asynchResultOne.size() < synchResultOne.size() / 2;
                  }

                  @Override
                  public void end() {
                    endOneCalled.set(true);

                    db
                        .command(
                            new SQLAsynchQuery<EntityImpl>(
                                sqlTwo,
                                new CommandResultListener() {
                                  @Override
                                  public boolean result(DatabaseSessionInternal db,
                                      Object iRecord) {
                                    asynchResultTwo.add((EntityImpl) iRecord);
                                    return true;
                                  }

                                  @Override
                                  public void end() {
                                    endTwoCalled.set(true);
                                  }

                                  @Override
                                  public Object getResult() {
                                    return null;
                                  }
                                }))
                        .execute(db);
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }))
        .execute(db);

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(
        EntityHelper.compareCollections(
            db,
            synchResultOne.subList(0, synchResultOne.size() / 2),
            db,
            asynchResultOne,
            null));
    Assert.assertTrue(
        EntityHelper.compareCollections(
            db, synchResultTwo, db, asynchResultTwo, null));
  }

  @Test
  public void queryOrderByRidDesc() {
    var resultSet = executeQuery("select from OUser order by @rid desc", db);

    Assert.assertFalse(resultSet.isEmpty());

    RID lastRid = null;
    for (var d : resultSet) {
      var rid = d.getIdentity().orElseThrow();

      if (lastRid != null) {
        Assert.assertTrue(rid.compareTo(lastRid) < 0);
      }
      lastRid = rid;
    }

    EntityImpl res =
        db.command(new CommandSQL("explain select from OUser order by @rid desc"))
            .execute(db);
    Assert.assertNull(res.field("orderByElapsed"));
  }

  public void testQueryParameterNotPersistent() {
    var doc = ((EntityImpl) db.newEntity());
    doc.field("test", "test");
    db.query("select from OUser where @rid = ?", doc).close();
    Assert.assertTrue(doc.isDirty());
  }

  public void testQueryLetExecutedOnce() {
    final List<Identifiable> result =
        db.query(
            new SQLSynchQuery<Identifiable>(
                "select name, $counter as counter from OUser let $counter = eval(\"$counter +"
                    + " 1\")"));

    Assert.assertFalse(result.isEmpty());
    var i = 1;
    for (var r : result) {
      Assert.assertEquals(((EntityImpl) r.getRecord(db)).<Object>field("counter"), i++);
    }
  }

  @Test
  public void testMultipleClustersWithPagination() throws Exception {
    final var cls = db.getMetadata().getSchema()
        .createClass("PersonMultipleClusters");
    try {
      Set<String> names =
          new HashSet<String>(Arrays.asList("Luca", "Jill", "Sara", "Tania", "Gianluca", "Marco"));
      for (var n : names) {
        db.begin();
        ((EntityImpl) db.newEntity("PersonMultipleClusters")).field("First", n).save();
        db.commit();
      }

      var query =
          new SQLSynchQuery<EntityImpl>(
              "select from PersonMultipleClusters where @rid > ? limit 2");
      List<EntityImpl> resultset = db.query(query, new ChangeableRecordId());

      while (!resultset.isEmpty()) {
        final RID last = resultset.getLast().getIdentity();
        for (var personDoc : resultset) {
          Assert.assertTrue(names.contains(personDoc.<String>getProperty("First")));
          Assert.assertTrue(names.remove(personDoc.<String>getProperty("First")));
        }

        resultset = db.query(query, last);
      }

      Assert.assertTrue(names.isEmpty());

    } finally {
      db.getMetadata().getSchema().dropClass("PersonMultipleClusters");
    }
  }

  @Test
  public void testOutFilterInclude() {
    Schema schema = db.getMetadata().getSchema();
    schema.createClass("TestOutFilterInclude", schema.getClass("V"));
    db.command("create class linkedToOutFilterInclude extends E").close();
    db.command("insert into TestOutFilterInclude content { \"name\": \"one\" }").close();
    db.command("insert into TestOutFilterInclude content { \"name\": \"two\" }").close();
    db
        .command(
            "create edge linkedToOutFilterInclude from (select from TestOutFilterInclude where name"
                + " = 'one') to (select from TestOutFilterInclude where name = 'two')")
        .close();

    final List<Identifiable> result =
        db.query(
            new SQLSynchQuery<Identifiable>(
                "select"
                    + " expand(out('linkedToOutFilterInclude')[@class='TestOutFilterInclude'].include('@rid'))"
                    + " from TestOutFilterInclude where name = 'one'"));

    Assert.assertEquals(result.size(), 1);

    for (var r : result) {
      Assert.assertNull(((EntityImpl) r.getRecord(db)).field("name"));
    }
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final RecordIteratorCluster<EntityImpl> iteratorCluster =
        db.browseCluster(db.getClusterNameById(clusterId));

    for (var i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext()) {
        break;
      }

      var doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  @Test
  public void testBinaryClusterSelect() {
    db.command("create blob cluster binarycluster").close();
    db.reload();
    var bytes = db.newBlob(new byte[]{1, 2, 3});

    db.begin();
    db.save(bytes, "binarycluster");
    db.commit();

    List<Identifiable> result =
        db.query(new SQLSynchQuery<Identifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 1);

    db.command("delete from cluster:binarycluster").close();

    result = db.query(
        new SQLSynchQuery<Identifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testExpandSkip() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    final var cls = schema.createClass("TestExpandSkip", v);
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createIndex(db, "TestExpandSkip.name", INDEX_TYPE.UNIQUE, "name");
    db.command("CREATE VERTEX TestExpandSkip set name = '1'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '2'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '3'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '4'").close();

    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestExpandSkip WHERE name = '1') to (SELECT FROM"
                + " TestExpandSkip WHERE name <> '1')")
        .close();

    var result =
        db.query("select expand(out()) from TestExpandSkip where name = '1'").toList();
    Assert.assertEquals(result.size(), 3);

    Map<Object, Object> params = new HashMap<Object, Object>();
    params.put("values", Arrays.asList("2", "3", "antani"));
    result =
        db
            .query(
                "select expand(out()[name in :values]) from TestExpandSkip where name = '1'",
                params).toList();
    Assert.assertEquals(result.size(), 2);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 1").toList();
    Assert.assertEquals(result.size(), 2);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 2").toList();
    Assert.assertEquals(result.size(), 1);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 3").toList();
    Assert.assertEquals(result.size(), 0);

    result =
        db
            .query("select expand(out()) from TestExpandSkip where name = '1' skip 1 limit 1")
            .toList();
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPolymorphicEdges() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var e = schema.getClass("E");
    final var v1 = schema.createClass("TestPolymorphicEdges_V", v);
    final var e1 = schema.createClass("TestPolymorphicEdges_E1", e);
    final var e2 = schema.createClass("TestPolymorphicEdges_E2", e1);

    db.command("CREATE VERTEX TestPolymorphicEdges_V set name = '1'").close();
    db.command("CREATE VERTEX TestPolymorphicEdges_V set name = '2'").close();
    db.command("CREATE VERTEX TestPolymorphicEdges_V set name = '3'").close();

    db
        .command(
            "CREATE EDGE TestPolymorphicEdges_E1 FROM (SELECT FROM TestPolymorphicEdges_V WHERE"
                + " name = '1') to (SELECT FROM TestPolymorphicEdges_V WHERE name = '2')")
        .close();
    db
        .command(
            "CREATE EDGE TestPolymorphicEdges_E2 FROM (SELECT FROM TestPolymorphicEdges_V WHERE"
                + " name = '1') to (SELECT FROM TestPolymorphicEdges_V WHERE name = '3')")
        .close();

    var result =
        db
            .query(
                "select expand(out('TestPolymorphicEdges_E1')) from TestPolymorphicEdges_V where"
                    + " name = '1'")
            .toList();
    Assert.assertEquals(result.size(), 2);

    result =
        db
            .query(
                "select expand(out('TestPolymorphicEdges_E2')) from TestPolymorphicEdges_V where"
                    + " name = '1' ")
            .toList();
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testSizeOfLink() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    final var cls = schema.createClass("TestSizeOfLink", v);
    db.command("CREATE VERTEX TestSizeOfLink set name = '1'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '2'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '3'").close();
    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestSizeOfLink WHERE name = '1') to (SELECT FROM"
                + " TestSizeOfLink WHERE name <> '1')")
        .close();

    var result =
        db
            .query(
                " select from (select from TestSizeOfLink where name = '1') where"
                    + " out()[name=2].size() > 0")
            .toList();
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testEmbeddedMapAndDotNotation() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    final var cls = schema.createClass("EmbeddedMapAndDotNotation", v);
    db.command("CREATE VERTEX EmbeddedMapAndDotNotation set name = 'foo'").close();
    db
        .command(
            "CREATE VERTEX EmbeddedMapAndDotNotation set data = {\"bar\": \"baz\", \"quux\":"
                + " 1}, name = 'bar'")
        .close();
    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'foo') to"
                + " (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'bar')")
        .close();

    List<Identifiable> result =
        db.query(
            new SQLSynchQuery<Identifiable>(
                " select out().data as result from (select from EmbeddedMapAndDotNotation where"
                    + " name = 'foo')"));
    Assert.assertEquals(result.size(), 1);
    EntityImpl doc = result.getFirst().getRecord(db);
    Assert.assertNotNull(doc);
    List list = doc.field("result");
    Assert.assertEquals(list.size(), 1);
    var first = list.getFirst();
    Assert.assertTrue(first instanceof Map);
    Assert.assertEquals(((Map) first).get("bar"), "baz");
  }

  @Test
  public void testLetWithQuotedValue() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    final var cls = schema.createClass("LetWithQuotedValue", v);
    db.command("CREATE VERTEX LetWithQuotedValue set name = \"\\\"foo\\\"\"").close();

    List<Identifiable> result =
        db.query(
            new SQLSynchQuery<Identifiable>(
                " select expand($a) let $a = (select from LetWithQuotedValue where name ="
                    + " \"\\\"foo\\\"\")"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testNestedProjection1() {
    var className = this.getClass().getSimpleName() + "_testNestedProjection1";
    db.command("create class " + className).close();

    db.begin();
    var elem1 = db.newEntity(className);
    elem1.setProperty("name", "a");
    elem1.save();

    var elem2 = db.newEntity(className);
    elem2.setProperty("name", "b");
    elem2.setProperty("surname", "lkj");
    elem2.save();

    var elem3 = db.newEntity(className);
    elem3.setProperty("name", "c");
    elem3.save();

    var elem4 = db.newEntity(className);
    elem4.setProperty("name", "d");
    elem4.setProperty("elem1", elem1);
    elem4.setProperty("elem2", elem2);
    elem4.setProperty("elem3", elem3);
    elem4.save();
    db.commit();

    var result =
        db.query(
            "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    Assert.assertTrue(result.hasNext());
    var item = result.next();
    Assert.assertNotNull(item);
    Assert.assertTrue(item.getProperty("elem1") instanceof Result);
    Assert.assertEquals(((Result) item.getProperty("elem1")).getProperty("name"), "a");
    Assert.assertEquals(((Result) item.getProperty("elem2")).getProperty("name"), "b");
    Assert.assertNull(((Result) item.getProperty("elem2")).getProperty("surname"));
    result.close();
  }

  @Override
  protected List<Result> executeQuery(String sql, DatabaseSessionInternal db,
      Object... args) {
    var rs = db.query(sql, args);
    return rs.toList();
  }
}
