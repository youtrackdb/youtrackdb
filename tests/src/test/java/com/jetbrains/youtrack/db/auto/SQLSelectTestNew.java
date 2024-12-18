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
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLAsynchQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-select")
@SuppressWarnings("unchecked")
public class SQLSelectTestNew extends AbstractSelectTest {

  private EntityImpl record = ((EntityImpl) db.newEntity());

  @Parameters(value = "remote")
  public SQLSelectTestNew(boolean remote) throws Exception {
    super(remote);
  }

  @BeforeClass
  public void init() {
    if (!db.getMetadata().getSchema().existsClass("Profile")) {
      db.getMetadata().getSchema().createClass("Profile", 1);

      for (int i = 0; i < 1000; ++i) {
        db.begin();
        db.<EntityImpl>newInstance("Profile").field("test", i).field("name", "N" + i)
            .save();
        db.commit();
      }
    }

    if (!db.getMetadata().getSchema().existsClass("company")) {
      db.getMetadata().getSchema().createClass("company", 1);
      for (int i = 0; i < 20; ++i) {
        db.begin();
        ((EntityImpl) db.newEntity("company")).field("id", i).save();
        db.commit();
      }
    }

    db.getMetadata().getSchema().getOrCreateClass("Account");
  }

  @Test
  public void queryNoDirtyResultset() {
    List<EntityImpl> result = executeQuery(" select from Profile ", db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertFalse(d.isDirty());
    }
  }

  @Test
  public void queryNoWhere() {
    List<EntityImpl> result = executeQuery(" select from Profile ", db);
    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void queryParentesisAsRight() {
    List<EntityImpl> result =
        executeQuery(
            "  select from Profile where ( name = 'Giuseppe' and ( name <> 'Napoleone' and nick is"
                + " not null ))  ",
            db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void querySingleAndDoubleQuotes() {
    List<EntityImpl> result = executeQuery("select from Profile where name = 'Giuseppe'",
        db);

    final int count = result.size();
    Assert.assertTrue(result.size() != 0);

    result = executeQuery("select from Profile where name = \"Giuseppe\"", db);
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.size(), count);
  }

  @Test
  public void queryTwoParentesisConditions() {
    List<EntityImpl> result =
        executeQuery(
            "select from Profile  where ( name = 'Giuseppe' and nick is not null ) or ( name ="
                + " 'Napoleone' and nick is not null ) ",
            db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
  }

  @Test
  public void testQueryCount() {
    db.getMetadata().reload();
    final long vertexesCount = db.countClass("V");
    List<Result> result =
        db.query("select count(*) as count from V").stream().collect(Collectors.toList());
    Assert.assertEquals(result.get(0).<Object>getProperty("count"), vertexesCount);
  }

  @Test
  public void querySchemaAndLike() {
    List<EntityImpl> result1 =
        executeQuery("select * from cluster:profile where name like 'Gi%'", db);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().startsWith("Gi"));
    }

    List<EntityImpl> result2 =
        executeQuery("select * from cluster:profile where name like '%epp%'", db);

    Assert.assertEquals(result1, result2);

    List<EntityImpl> result3 =
        executeQuery("select * from cluster:profile where name like 'Gius%pe'", db);

    Assert.assertEquals(result1, result3);

    result1 = executeQuery("select * from cluster:profile where name like '%Gi%'", db);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }

    result1 = executeQuery("select * from cluster:profile where name like ?", db, "%Gi%");

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

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
    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("tags", tags, PropertyType.EMBEDDEDSET);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery("select from Profile where tags CONTAINS 'smart'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

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
    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("tags", tags);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery("select from Profile where tags[0] = 'smart'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery("select from Profile where tags[0,1] CONTAINSALL ['smart','nice']", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentSet() {
    HashSet<EntityImpl> coll = new HashSet<>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("coll", coll, PropertyType.EMBEDDEDSET);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertTrue(resultset.get(0).field("value") instanceof Collection);

    Collection xcoll = resultset.get(0).field("value");
    Assert.assertEquals(((Entity) xcoll.iterator().next()).getProperty("name"), "Jay");

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentList() {
    List<EntityImpl> coll = new ArrayList<>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("coll", coll, PropertyType.EMBEDDEDLIST);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    //    Assert.assertEquals(resultset.get(0).field("value").getClass(), EntityImpl.class);
    Object result = resultset.get(0).field("value");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(((Collection) result).size(), 1);
    Assert.assertEquals(
        ((Entity) ((Collection) result).iterator().next()).getProperty("name"), "Jay");

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
    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery("select from Profile where customReferences CONTAINSKEY 'first'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences CONTAINSVALUE ( name like 'Ja%')",
            db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences['second']['name'] like 'Ja%'", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select customReferences['second', 'first'] as customReferences from Profile where"
                + " customReferences.size() = 2",
            db);
    Assert.assertEquals(resultset.size(), 1);

    if (resultset.get(0).field("customReferences").getClass().isArray()) {
      Object[] customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.length, 2);
      Assert.assertTrue(customReferencesBack[0] instanceof EntityImpl);
      Assert.assertTrue(customReferencesBack[1] instanceof EntityImpl);
    } else if (resultset.get(0).field("customReferences") instanceof List) {
      List<EntityImpl> customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.size(), 2);
      Assert.assertTrue(customReferencesBack.get(0) instanceof EntityImpl);
      Assert.assertTrue(customReferencesBack.get(1) instanceof EntityImpl);
    } else {
      Assert.fail("Wrong type received: " + resultset.get(0).field("customReferences"));
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

    EntityImpl doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    doc.save();
    db.commit();

    List<EntityImpl> resultset =
        executeQuery(
            "select from Profile where customReferences.keys() CONTAINS 'first'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences.values() CONTAINS ( name like 'Ja%')",
            db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    db.begin();
    doc.delete();
    db.commit();
  }

  @Test
  public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
    List<EntityImpl> result =
        executeQuery(
            "select * from cluster:profile where races contains"
                + " (name.toLowerCase(Locale.ENGLISH).subString(0,1) = 'e')",
            db);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertNotNull(record.field("races"));

      Collection<EntityImpl> races = record.field("races");
      boolean found = false;
      for (EntityImpl race : races) {
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
    record = ((EntityImpl) db.newEntity());
    record.setClassName("Animal");
    record.field("name", "Cat");

    db.begin();
    Collection<EntityImpl> races = new HashSet<EntityImpl>();
    races.add(((EntityImpl) db.newInstance("AnimalRace")).field("name", "European"));
    races.add(((EntityImpl) db.newInstance("AnimalRace")).field("name", "Siamese"));
    record.field("age", 10);
    record.field("races", races);
    record.save();
    db.commit();

    List<EntityImpl> result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['European','Asiatic'])",
            db);

    boolean found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (EntityImpl race : races) {
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
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (EntityImpl race : races) {
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
    record = ((EntityImpl) db.newEntity());
    record.setClassName("Animal");
    record.field("name", "Cat");

    Collection<Integer> rates = new HashSet<Integer>();
    rates.add(100);
    rates.add(200);
    record.field("rates", rates);

    db.begin();
    record.save("animal");
    db.commit();

    List<EntityImpl> result =
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
    int clusterId = db.getMetadata().getSchema().getClass("ORole").getClusterIds()[0];
    List<Long> positions = getValidPositions(clusterId);

    List<EntityImpl> result =
        executeQuery(
            "select * from OUser where roles contains #" + clusterId + ":" + positions.get(0),
            db);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryWhereInpreparred() {
    List<EntityImpl> result =
        executeQuery("select * from OUser where name in [ :name ]", db, "admin");

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((EntityImpl) result.get(0).getRecord(db)).field("name"), "admin");
  }

  @Test
  public void queryAllOperator() {
    List<EntityImpl> result = executeQuery("select from Account where all() is null", db);

    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void queryOrderBy() {
    List<EntityImpl> result = executeQuery("select from Profile order by name", db);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    boolean isNullSegment = true; // NULL VALUES AT THE BEGINNING!
    for (EntityImpl d : result) {
      final String fieldValue = d.field("name");
      if (fieldValue != null) {
        isNullSegment = false;
      } else {
        Assert.assertTrue(isNullSegment);
      }

      if (lastName != null && fieldValue != null) {
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
    } catch (CommandSQLParsingException e) {
    }
  }

  @Test
  public void queryLimitOnly() {
    List<EntityImpl> result = executeQuery("select from Profile limit 1", db);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void querySkipOnly() {
    List<EntityImpl> result = executeQuery("select from Profile", db);
    int total = result.size();

    result = executeQuery("select from Profile skip 1", db);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithSkipAndLimit() {
    List<EntityImpl> result = executeQuery("select from Profile", db);

    List<EntityImpl> page = executeQuery("select from Profile skip 10 limit 10", db);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOffsetOnly() {
    List<EntityImpl> result = executeQuery("select from Profile", db);
    int total = result.size();

    result = executeQuery("select from Profile offset 1", db);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithOffsetAndLimit() {
    List<EntityImpl> result = executeQuery("select from Profile", db);

    List<EntityImpl> page = executeQuery("select from Profile offset 10 limit 10", db);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderBySkipAndLimit() {
    List<EntityImpl> result = executeQuery("select from Profile order by name", db);

    List<EntityImpl> page =
        executeQuery("select from Profile order by name limit 10 skip 10", db);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderByDescSkipAndLimit() {
    List<EntityImpl> result = executeQuery("select from Profile order by name desc", db);

    List<EntityImpl> page =
        executeQuery("select from Profile order by name desc limit 10 skip 10", db);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOrderByAndLimit() {
    List<EntityImpl> result = executeQuery("select from Profile order by name limit 2", db);

    Assert.assertTrue(result.size() <= 2);

    String lastName = null;
    for (EntityImpl d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionAndOrderBy() {
    List<EntityImpl> result =
        executeQuery("select from Profile where name is not null order by name", db);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (EntityImpl d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionsAndOrderBy() {
    List<EntityImpl> result =
        executeQuery(
            "select from Profile where name is not null order by name desc, id asc", db);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (EntityImpl d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) <= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryRecordTargetRid() {
    int profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    List<Long> positions = getValidPositions(profileClusterId);

    List<EntityImpl> result =
        executeQuery("select from " + profileClusterId + ":" + positions.get(0), db);

    Assert.assertEquals(result.size(), 1);

    for (EntityImpl d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    }
  }

  @Test
  public void queryRecordTargetRids() {
    int profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    List<Long> positions = getValidPositions(profileClusterId);

    List<EntityImpl> result =
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

    int profileClusterId =
        db.getMetadata().getSchema().getClass("Profile").getClusterIds()[0];
    List<Long> postions = getValidPositions(profileClusterId);

    List<EntityImpl> result =
        executeQuery(
            "select from Profile where @rid = #" + profileClusterId + ":" + postions.get(0),
            db);

    Assert.assertEquals(result.size(), 1);

    for (EntityImpl d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + postions.get(0));
    }
  }

  @Test
  public void queryRecordAttribClass() {
    List<EntityImpl> result = executeQuery("select from Profile where @class = 'Profile'",
        db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertEquals(d.getClassName(), "Profile");
    }
  }

  @Test
  public void queryRecordAttribVersion() {
    List<EntityImpl> result = executeQuery("select from Profile where @version > 0", db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertTrue(d.getVersion() > 0);
    }
  }

  @Test
  public void queryRecordAttribSize() {
    List<EntityImpl> result = executeQuery("select from Profile where @size >= 50", db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertTrue(d.toStream().length >= 50);
    }
  }

  @Test
  public void queryRecordAttribType() {
    List<EntityImpl> result = executeQuery("select from Profile where @type = 'document'",
        db);

    Assert.assertTrue(result.size() != 0);

    for (EntityImpl d : result) {
      Assert.assertEquals(RecordInternal.getRecordType(db, d), EntityImpl.RECORD_TYPE);
    }
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
    long tot = db.countClass("V");

    int count = 0;
    for (EntityImpl record : db.browseClass("V")) {
      count++;
    }

    Assert.assertEquals(count, tot);

    Assert.assertTrue(executeQuery("select from V", db).size() >= tot);
  }

  @Test
  public void queryWithManualPagination() {
    RID last = new ChangeableRecordId();
    List<EntityImpl> resultset =
        executeQuery("select from Profile where @rid > ? LIMIT 3", db, last);

    int iterationCount = 0;
    Assert.assertFalse(resultset.isEmpty());
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() < 0
                || (d.getIdentity().getClusterId() >= last.getClusterId())
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = executeQuery("select from Profile where @rid > ? LIMIT 3", db, last);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPagination() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>("select from Profile LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query);

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationAndRidInWhere() {
    int clusterId = db.getClusterIdByName("profile");

    long[] range = db.getStorage().getClusterDataRange(db, clusterId);

    final SQLSynchQuery<EntityImpl> query =
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

    Assert.assertEquals(resultset.get(0).getIdentity(), new RecordId(clusterId, range[0]));

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertEquals(last, new RecordId(clusterId, range[1]));
    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhere() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > 0 LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      // System.out.printf("\nIterating page %d, last record is %s", iterationCount, last);

      iterationCount++;
      resultset = db.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVar() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = db.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVarAtTheFirstQueryCall() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");
    RID last = new ChangeableRecordId();

    List<EntityImpl> resultset = db.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (EntityImpl d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = db.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAbsenceOfAutomaticPaginationBecauseOfBindingVarReset() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select from Profile where followers.length() > ? LIMIT 3");

    List<EntityImpl> resultset = db.query(query, -1);

    final RID firstRidFirstQuery = resultset.get(0).getIdentity();

    resultset = db.query(query, -2);

    final RID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void includeFields() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>("select expand( roles.include('name') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (EntityImpl d : resultset) {
      Assert.assertTrue(d.fields() <= 1);
      if (d.fields() == 1) {
        Assert.assertTrue(d.containsField("name"));
      }
    }
  }

  @Test
  public void excludeFields() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>("select expand( roles.exclude('rules') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (EntityImpl d : resultset) {
      Assert.assertFalse(d.containsField("rules"));
    }
  }

  @Test
  public void excludeAttributes() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select expand( roles.exclude('@rid', '@class') ) from OUser");

    List<EntityImpl> resultset = db.query(query);

    for (EntityImpl d : resultset) {
      Assert.assertFalse(d.getIdentity().isPersistent());
      Assert.assertNull(d.getSchemaClass());
    }
  }

  @Test
  public void queryResetPagination() {
    final SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>("select from Profile LIMIT 3");

    List<EntityImpl> resultset = db.query(query);
    final RID firstRidFirstQuery = resultset.get(0).getIdentity();
    query.resetPagination();

    resultset = db.query(query);
    final RID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void queryBetween() {
    List<EntityImpl> result =
        executeQuery("select * from account where nr between 10 and 20", db);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(
          ((Integer) record.field("nr")) >= 10 && ((Integer) record.field("nr")) <= 20);
    }
  }

  @Test
  public void queryParenthesisInStrings() {

    db.command("INSERT INTO account (name) VALUES ('test (demo)')").close();

    List<EntityImpl> result =
        executeQuery("select * from account where name = 'test (demo)'", db);

    Assert.assertEquals(result.size(), 1);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);
      Assert.assertEquals(record.field("name"), "test (demo)");
    }
  }

  @Test
  public void queryMathOperators() {
    List<EntityImpl> result = executeQuery("select * from account where id < 3 + 4", db);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 3 + 4);
    }

    result = executeQuery("select * from account where id < 10 - 3", db);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 10 - 3);
    }

    result = executeQuery("select * from account where id < 3 * 2", db);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 3 * 2);
    }

    result = executeQuery("select * from account where id < 120 / 20", db);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 120 / 20);
    }

    result = executeQuery("select * from account where id < 27 % 10", db);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 27 % 10);
    }

    result = executeQuery("select * from account where id = id * 1", db);
    Assert.assertFalse(result.isEmpty());

    List<EntityImpl> result2 =
        executeQuery("select count(*) as tot from account where id >= 0", db);
    Assert.assertEquals(result.size(), ((Number) result2.get(0).field("tot")).intValue());
  }

  @Test
  public void testBetweenWithParameters() {

    final List<EntityImpl> result =
        executeQuery(
            "select * from company where id between ? and ? and salary is not null",
            db,
            4,
            7);

    System.out.println("testBetweenWithParameters:");
    for (EntityImpl d : result) {
      System.out.println(d);
    }

    Assert.assertEquals(result.size(), 4, "Found: " + result);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final EntityImpl record : result) {
      resultsList.remove(record.<Integer>field("id"));
    }
  }

  @Test
  public void testInWithParameters() {

    final List<EntityImpl> result =
        executeQuery(
            "select * from company where id in [?, ?, ?, ?] and salary is not null",
            db,
            4,
            5,
            6,
            7);

    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final EntityImpl record : result) {
      resultsList.remove(record.<Integer>field("id"));
    }
  }

  @Test
  public void testEqualsNamedParameter() {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<EntityImpl> result =
        executeQuery(
            "select * from company where id = :id and salary is not null", db, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testQueryAsClass() {

    List<EntityImpl> result =
        executeQuery("select from Account where addresses.@class in [ 'Address' ]", db);
    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((EntityImpl)
              ((Collection<Identifiable>) d.field("addresses")).iterator().next().getRecord(db))
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  @Test
  public void testQueryNotOperator() {

    List<EntityImpl> result =
        executeQuery(
            "select from Account where not ( addresses.@class in [ 'Address' ] )", db);
    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      Assert.assertTrue(
          d.field("addresses") == null
              || ((Collection<Identifiable>) d.field("addresses")).isEmpty()
              || !((EntityImpl)
              ((Collection<Identifiable>) d.field("addresses"))
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
    List<EntityImpl> result =
        executeQuery(
            "select from Account where addresses[@class='Address'][city.country.name] ="
                + " 'Washington'",
            db);
    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((EntityImpl)
              ((Collection<Identifiable>) d.field("addresses")).iterator().next().getRecord(db))
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  public void testParams() {
    SchemaClass test = db.getMetadata().getSchema().getClass("test");
    if (test == null) {
      test = db.getMetadata().getSchema().createClass("test");
      test.createProperty(db, "f1", PropertyType.STRING);
      test.createProperty(db, "f2", PropertyType.STRING);
    }
    EntityImpl document = ((EntityImpl) db.newEntity(test));
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
    List<EntityImpl> result = executeQuery("select from Account", db);

    Assert.assertTrue(result.size() != 0);

    List<EntityImpl> result2 =
        executeQuery("select from Account where @this instanceof 'Account'", db);

    Assert.assertEquals(result2.size(), result.size());

    List<EntityImpl> result3 =
        executeQuery("select from Account where @class instanceof 'Account'", db);

    Assert.assertEquals(result3.size(), result.size());
  }

  @Test
  public void subQuery() {
    List<EntityImpl> result =
        executeQuery(
            "select from Account where name in ( select name from Account where name is not null"
                + " limit 1 )",
            db);

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void subQueryNoFrom() {
    List<EntityImpl> result2 =
        executeQuery(
            "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
                + " addresses.size() > 0 )",
            db);

    Assert.assertTrue(result2.size() != 0);
    Assert.assertTrue(result2.get(0).field("$names") instanceof Collection<?>);
    Assert.assertFalse(((Collection<?>) result2.get(0).field("$names")).isEmpty());
  }

  @Test
  public void subQueryLetAndIndexedWhere() {
    List<EntityImpl> result =
        executeQuery("select $now from OUser let $now = eval('42') where name = 'admin'", db);

    Assert.assertEquals(result.size(), 1);
    Assert.assertNotNull(result.get(0).field("$now"), result.get(0).toString());
  }

  @Test
  public void queryOrderByWithLimit() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass facClass = schema.getClass("FicheAppelCDI");
    if (facClass == null) {
      facClass = schema.createClass("FicheAppelCDI");
    }
    if (!facClass.existsProperty("date")) {
      facClass.createProperty(db, "date", PropertyType.DATE);
    }

    final Calendar currentYear = Calendar.getInstance();
    final Calendar oneYearAgo = Calendar.getInstance();
    oneYearAgo.add(Calendar.YEAR, -1);

    db.begin();
    EntityImpl doc1 = ((EntityImpl) db.newEntity(facClass));
    doc1.field("context", "test");
    doc1.field("date", currentYear.getTime());
    doc1.save();

    EntityImpl doc2 = ((EntityImpl) db.newEntity(facClass));
    doc2.field("context", "test");
    doc2.field("date", oneYearAgo.getTime());
    doc2.save();
    db.commit();

    List<EntityImpl> result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select * from " + facClass.getName() + " where context = 'test' order by date",
                1));

    Calendar smaller = Calendar.getInstance();
    smaller.setTime(result.get(0).field("date", Date.class));
    Assert.assertEquals(smaller.get(Calendar.YEAR), oneYearAgo.get(Calendar.YEAR));

    result =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select * from "
                    + facClass.getName()
                    + " where context = 'test' order by date DESC",
                1));

    Calendar bigger = Calendar.getInstance();
    bigger.setTime(result.get(0).field("date", Date.class));
    Assert.assertEquals(bigger.get(Calendar.YEAR), currentYear.get(Calendar.YEAR));
  }

  @Test
  public void queryWithTwoRidInWhere() {
    int clusterId = db.getClusterIdByName("profile");

    List<Long> positions = getValidPositions(clusterId);

    final long minPos;
    final long maxPos;
    if (positions.get(5).compareTo(positions.get(25)) > 0) {
      minPos = positions.get(25);
      maxPos = positions.get(5);
    } else {
      minPos = positions.get(5);
      maxPos = positions.get(25);
    }

    List<EntityImpl> resultset =
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

    Assert.assertEquals(resultset.get(0).field("oid"),
        new RecordId(clusterId, maxPos).toString());
  }

  @Test
  public void testSelectFromListParameter() {
    SchemaClass placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(db, "id", PropertyType.STRING);
    placeClass.createProperty(db, "descr", PropertyType.STRING);
    placeClass.createIndex(db, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    EntityImpl odoc = ((EntityImpl) db.newEntity("Place"));
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

    List<EntityImpl> result = executeQuery("select from place where id in :place", db,
        params);
    Assert.assertEquals(1, result.size());

    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidFromListParameter() {
    SchemaClass placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(db, "id", PropertyType.STRING);
    placeClass.createProperty(db, "descr", PropertyType.STRING);
    placeClass.createIndex(db, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    List<RID> inputValues = new ArrayList<RID>();

    EntityImpl odoc = ((EntityImpl) db.newEntity("Place"));
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

    List<EntityImpl> result =
        executeQuery("select from place where @rid in :place", db, params);
    Assert.assertEquals(2, result.size());

    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidInList() {
    SchemaClass placeClass = db.getMetadata().getSchema().createClass("Place", 1);
    db.getMetadata().getSchema().createClass("FamousPlace", 1, placeClass);

    EntityImpl firstPlace = ((EntityImpl) db.newEntity("Place"));

    db.begin();
    db.save(firstPlace);
    EntityImpl secondPlace = ((EntityImpl) db.newEntity("Place"));
    db.save(secondPlace);
    EntityImpl famousPlace = ((EntityImpl) db.newEntity("FamousPlace"));
    db.save(famousPlace);
    db.commit();

    RID secondPlaceId = secondPlace.getIdentity();
    RID famousPlaceId = famousPlace.getIdentity();
    // if one of these two asserts fails, the test will be meaningless.
    Assert.assertTrue(secondPlaceId.getClusterId() < famousPlaceId.getClusterId());
    Assert.assertTrue(secondPlaceId.getClusterPosition() > famousPlaceId.getClusterPosition());

    List<EntityImpl> result =
        executeQuery(
            "select from Place where @rid in [" + secondPlaceId + "," + famousPlaceId + "]",
            db);
    Assert.assertEquals(2, result.size());

    db.getMetadata().getSchema().dropClass("FamousPlace");
    db.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testMapKeys() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<EntityImpl> result =
        executeQuery(
            "select * from company where id = :id and salary is not null", db, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAsynch() {
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<EntityImpl> synchResultOne =
        db.command(new SQLSynchQuery<EntityImpl>(sqlOne)).execute(db);
    final List<EntityImpl> synchResultTwo =
        db.command(new SQLSynchQuery<EntityImpl>(sqlTwo)).execute(db);

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<EntityImpl> asynchResultOne = new ArrayList<EntityImpl>();
    final List<EntityImpl> asynchResultTwo = new ArrayList<EntityImpl>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

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
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<EntityImpl> synchResultOne =
        db.command(new SQLSynchQuery<EntityImpl>(sqlOne)).execute(db);
    final List<EntityImpl> synchResultTwo =
        db.command(new SQLSynchQuery<EntityImpl>(sqlTwo)).execute(db);

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<EntityImpl> asynchResultOne = new ArrayList<EntityImpl>();
    final List<EntityImpl> asynchResultTwo = new ArrayList<EntityImpl>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

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
    List<EntityImpl> result = executeQuery("select from OUser order by @rid desc", db);

    Assert.assertFalse(result.isEmpty());

    RID lastRid = null;
    for (EntityImpl d : result) {
      RID rid = d.getIdentity();

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

  @Test
  public void testSelectFromIndexValues() {
    db.command("create index selectFromIndexValues on Profile (name) notunique").close();

    final List<EntityImpl> classResult =
        new ArrayList<EntityImpl>(
            (List<EntityImpl>)
                db.query(
                    new SQLSynchQuery<EntityImpl>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<EntityImpl> indexValuesResult =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from indexvalues:selectFromIndexValues where ((nick like 'J%') or (nick"
                    + " like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (EntityImpl document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null) {
        Assert.assertTrue(lastName.compareTo(name) <= 0);
      }

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testSelectFromIndexValuesAsc() {
    db.command("create index selectFromIndexValuesAsc on Profile (name) notunique").close();

    final List<EntityImpl> classResult =
        new ArrayList<EntityImpl>(
            (List<EntityImpl>)
                db.query(
                    new SQLSynchQuery<EntityImpl>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<EntityImpl> indexValuesResult =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from indexvaluesasc:selectFromIndexValuesAsc where ((nick like 'J%') or"
                    + " (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (EntityImpl document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null) {
        Assert.assertTrue(lastName.compareTo(name) <= 0);
      }

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testSelectFromIndexValuesDesc() {
    db.command("create index selectFromIndexValuesDesc on Profile (name) notunique").close();

    final List<EntityImpl> classResult =
        new ArrayList<EntityImpl>(
            (List<EntityImpl>)
                db.query(
                    new SQLSynchQuery<EntityImpl>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<EntityImpl> indexValuesResult =
        db.query(
            new SQLSynchQuery<EntityImpl>(
                "select from indexvaluesdesc:selectFromIndexValuesDesc where ((nick like 'J%') or"
                    + " (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (EntityImpl document : indexValuesResult) {
      String name = document.field("name");

      if (lastName != null) {
        Assert.assertTrue(lastName.compareTo(name) >= 0);
      }

      lastName = name;
      Assert.assertTrue(classResult.remove(document));
    }

    Assert.assertTrue(classResult.isEmpty());
  }

  public void testQueryParameterNotPersistent() {
    EntityImpl doc = ((EntityImpl) db.newEntity());
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
    int i = 1;
    for (Identifiable r : result) {
      Assert.assertEquals(((EntityImpl) r.getRecord(db)).<Object>field("counter"), i++);
    }
  }

  @Test
  public void testMultipleClustersWithPagination() throws Exception {
    final SchemaClass cls = db.getMetadata().getSchema()
        .createClass("PersonMultipleClusters");
    cls.addCluster(db, "PersonMultipleClusters_1");
    cls.addCluster(db, "PersonMultipleClusters_2");
    cls.addCluster(db, "PersonMultipleClusters_3");
    cls.addCluster(db, "PersonMultipleClusters_4");

    try {
      Set<String> names =
          new HashSet<String>(Arrays.asList("Luca", "Jill", "Sara", "Tania", "Gianluca", "Marco"));
      for (String n : names) {
        db.begin();
        ((EntityImpl) db.newEntity("PersonMultipleClusters")).field("First", n).save();
        db.commit();
      }

      SQLSynchQuery<EntityImpl> query =
          new SQLSynchQuery<EntityImpl>(
              "select from PersonMultipleClusters where @rid > ? limit 2");
      List<EntityImpl> resultset = db.query(query, new ChangeableRecordId());

      while (!resultset.isEmpty()) {
        final RID last = resultset.get(resultset.size() - 1).getIdentity();

        for (EntityImpl personDoc : resultset) {
          Assert.assertTrue(names.contains(personDoc.field("First")));
          Assert.assertTrue(names.remove(personDoc.field("First")));
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

    for (Identifiable r : result) {
      Assert.assertNull(((EntityImpl) r.getRecord(db)).field("name"));
    }
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final RecordIteratorCluster<EntityImpl> iteratorCluster =
        db.browseCluster(db.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext()) {
        break;
      }

      EntityImpl doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  @Test
  public void testBinaryClusterSelect() {
    db.command("create blob cluster binarycluster").close();
    db.reload();
    Blob bytes = db.newBlob(new byte[]{1, 2, 3});

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
    SchemaClass v = schema.getClass("V");
    final SchemaClass cls = schema.createClass("TestExpandSkip", v);
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

    List<Identifiable> result =
        db.query("select expand(out()) from TestExpandSkip where name = '1'").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 3);

    Map<Object, Object> params = new HashMap<Object, Object>();
    params.put("values", Arrays.asList("2", "3", "antani"));
    result =
        db
            .query(
                "select expand(out()[name in :values]) from TestExpandSkip where name = '1'",
                params)
            .stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 1").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 2").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 3").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 0);

    result =
        db
            .query("select expand(out()) from TestExpandSkip where name = '1' skip 1 limit 1")
            .stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPolymorphicEdges() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    SchemaClass e = schema.getClass("E");
    final SchemaClass v1 = schema.createClass("TestPolymorphicEdges_V", v);
    final SchemaClass e1 = schema.createClass("TestPolymorphicEdges_E1", e);
    final SchemaClass e2 = schema.createClass("TestPolymorphicEdges_E2", e1);

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

    List<Identifiable> result =
        db
            .query(
                "select expand(out('TestPolymorphicEdges_E1')) from TestPolymorphicEdges_V where"
                    + " name = '1'")
            .stream()
            .map((r) -> r.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        db
            .query(
                "select expand(out('TestPolymorphicEdges_E2')) from TestPolymorphicEdges_V where"
                    + " name = '1' ")
            .stream()
            .map((r) -> r.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testSizeOfLink() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    final SchemaClass cls = schema.createClass("TestSizeOfLink", v);
    db.command("CREATE VERTEX TestSizeOfLink set name = '1'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '2'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '3'").close();
    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestSizeOfLink WHERE name = '1') to (SELECT FROM"
                + " TestSizeOfLink WHERE name <> '1')")
        .close();

    List<Identifiable> result =
        db
            .query(
                " select from (select from TestSizeOfLink where name = '1') where"
                    + " out()[name=2].size() > 0")
            .stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testEmbeddedMapAndDotNotation() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    final SchemaClass cls = schema.createClass("EmbeddedMapAndDotNotation", v);
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
    EntityImpl doc = result.get(0).getRecord(db);
    Assert.assertNotNull(doc);
    List list = doc.field("result");
    Assert.assertEquals(list.size(), 1);
    Object first = list.get(0);
    Assert.assertTrue(first instanceof Map);
    Assert.assertEquals(((Map) first).get("bar"), "baz");
  }

  @Test
  public void testLetWithQuotedValue() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass v = schema.getClass("V");
    final SchemaClass cls = schema.createClass("LetWithQuotedValue", v);
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
    String className = this.getClass().getSimpleName() + "_testNestedProjection1";
    db.command("create class " + className).close();

    db.begin();
    Entity elem1 = db.newEntity(className);
    elem1.setProperty("name", "a");
    elem1.save();

    Entity elem2 = db.newEntity(className);
    elem2.setProperty("name", "b");
    elem2.setProperty("surname", "lkj");
    elem2.save();

    Entity elem3 = db.newEntity(className);
    elem3.setProperty("name", "c");
    elem3.save();

    Entity elem4 = db.newEntity(className);
    elem4.setProperty("name", "d");
    elem4.setProperty("elem1", elem1);
    elem4.setProperty("elem2", elem2);
    elem4.setProperty("elem3", elem3);
    elem4.save();
    db.commit();

    ResultSet result =
        db.query(
            "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    Assert.assertTrue(result.hasNext());
    Result item = result.next();
    Assert.assertNotNull(item);
    Assert.assertTrue(item.getProperty("elem1") instanceof Result);
    Assert.assertEquals("a", ((Result) item.getProperty("elem1")).getProperty("name"));
    Assert.assertEquals("b", ((Result) item.getProperty("elem2")).getProperty("name"));
    Assert.assertNull(((Result) item.getProperty("elem2")).getProperty("surname"));
    result.close();
  }

  @Override
  protected List<EntityImpl> executeQuery(String sql, DatabaseSessionInternal db,
      Object... args) {
    ResultSet rs = db.query(sql, args);
    List<EntityImpl> result = new ArrayList<>();
    while (rs.hasNext()) {
      result.add((EntityImpl) rs.next().toEntity());
    }
    rs.close();
    return result;
  }
}
