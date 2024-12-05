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

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.ChangeableRecordId;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.YTCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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

  private YTDocument record = new YTDocument();

  @Parameters(value = "remote")
  public SQLSelectTestNew(boolean remote) throws Exception {
    super(remote);
  }

  @BeforeClass
  public void init() {
    if (!database.getMetadata().getSchema().existsClass("Profile")) {
      database.getMetadata().getSchema().createClass("Profile", 1);

      for (int i = 0; i < 1000; ++i) {
        database.begin();
        database.<YTDocument>newInstance("Profile").field("test", i).field("name", "N" + i).save();
        database.commit();
      }
    }

    if (!database.getMetadata().getSchema().existsClass("company")) {
      database.getMetadata().getSchema().createClass("company", 1);
      for (int i = 0; i < 20; ++i) {
        database.begin();
        new YTDocument("company").field("id", i).save();
        database.commit();
      }
    }

    database.getMetadata().getSchema().getOrCreateClass("Account");
  }

  @Test
  public void queryNoDirtyResultset() {
    List<YTDocument> result = executeQuery(" select from Profile ", database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertFalse(d.isDirty());
    }
  }

  @Test
  public void queryNoWhere() {
    List<YTDocument> result = executeQuery(" select from Profile ", database);
    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTDocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryParentesisAsRight() {
    List<YTDocument> result =
        executeQuery(
            "  select from Profile where ( name = 'Giuseppe' and ( name <> 'Napoleone' and nick is"
                + " not null ))  ",
            database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTDocument.RECORD_TYPE);
    }
  }

  @Test
  public void querySingleAndDoubleQuotes() {
    List<YTDocument> result = executeQuery("select from Profile where name = 'Giuseppe'", database);

    final int count = result.size();
    Assert.assertTrue(result.size() != 0);

    result = executeQuery("select from Profile where name = \"Giuseppe\"", database);
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.size(), count);
  }

  @Test
  public void queryTwoParentesisConditions() {
    List<YTDocument> result =
        executeQuery(
            "select from Profile  where ( name = 'Giuseppe' and nick is not null ) or ( name ="
                + " 'Napoleone' and nick is not null ) ",
            database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTDocument.RECORD_TYPE);
    }
  }

  @Test
  public void testQueryCount() {
    database.getMetadata().reload();
    final long vertexesCount = database.countClass("V");
    List<YTResult> result =
        database.query("select count(*) as count from V").stream().collect(Collectors.toList());
    Assert.assertEquals(result.get(0).<Object>getProperty("count"), vertexesCount);
  }

  @Test
  public void querySchemaAndLike() {
    List<YTDocument> result1 =
        executeQuery("select * from cluster:profile where name like 'Gi%'", database);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().startsWith("Gi"));
    }

    List<YTDocument> result2 =
        executeQuery("select * from cluster:profile where name like '%epp%'", database);

    Assert.assertEquals(result1, result2);

    List<YTDocument> result3 =
        executeQuery("select * from cluster:profile where name like 'Gius%pe'", database);

    Assert.assertEquals(result1, result3);

    result1 = executeQuery("select * from cluster:profile where name like '%Gi%'", database);

    for (int i = 0; i < result1.size(); ++i) {
      record = result1.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.field("name").toString().contains("Gi"));
    }

    result1 = executeQuery("select * from cluster:profile where name like ?", database, "%Gi%");

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

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("tags", tags, YTType.EMBEDDEDSET);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery("select from Profile where tags CONTAINS 'smart'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryContainsInEmbeddedList() {
    List<String> tags = new ArrayList<String>();
    tags.add("smart");
    tags.add("nice");

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("tags", tags);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery("select from Profile where tags[0] = 'smart'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery("select from Profile where tags[0,1] CONTAINSALL ['smart','nice']", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryContainsInDocumentSet() {
    HashSet<YTDocument> coll = new HashSet<YTDocument>();
    coll.add(new YTDocument("name", "Luca", "surname", "Garulli"));
    coll.add(new YTDocument("name", "Jay", "surname", "Miner"));

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("coll", coll, YTType.EMBEDDEDSET);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", database);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertTrue(resultset.get(0).field("value") instanceof Collection);

    Collection xcoll = resultset.get(0).field("value");
    Assert.assertEquals(((YTEntity) xcoll.iterator().next()).getProperty("name"), "Jay");

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryContainsInDocumentList() {
    List<YTDocument> coll = new ArrayList<YTDocument>();
    coll.add(new YTDocument("name", "Luca", "surname", "Garulli"));
    coll.add(new YTDocument("name", "Jay", "surname", "Miner"));

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("coll", coll, YTType.EMBEDDEDLIST);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", database);
    Assert.assertEquals(resultset.size(), 1);
    //    Assert.assertEquals(resultset.get(0).field("value").getClass(), YTDocument.class);
    Object result = resultset.get(0).field("value");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(((Collection) result).size(), 1);
    Assert.assertEquals(
        ((YTEntity) ((Collection) result).iterator().next()).getProperty("name"), "Jay");

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapClassic() {
    Map<String, YTDocument> customReferences = new HashMap<String, YTDocument>();
    customReferences.put("first", new YTDocument("name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new YTDocument("name", "Jay", "surname", "Miner"));

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("customReferences", customReferences, YTType.EMBEDDEDMAP);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery("select from Profile where customReferences CONTAINSKEY 'first'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences CONTAINSVALUE ( name like 'Ja%')",
            database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences['second']['name'] like 'Ja%'", database);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select customReferences['second', 'first'] as customReferences from Profile where"
                + " customReferences.size() = 2",
            database);
    Assert.assertEquals(resultset.size(), 1);

    if (resultset.get(0).field("customReferences").getClass().isArray()) {
      Object[] customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.length, 2);
      Assert.assertTrue(customReferencesBack[0] instanceof YTDocument);
      Assert.assertTrue(customReferencesBack[1] instanceof YTDocument);
    } else if (resultset.get(0).field("customReferences") instanceof List) {
      List<YTDocument> customReferencesBack = resultset.get(0).field("customReferences");
      Assert.assertEquals(customReferencesBack.size(), 2);
      Assert.assertTrue(customReferencesBack.get(0) instanceof YTDocument);
      Assert.assertTrue(customReferencesBack.get(1) instanceof YTDocument);
    } else {
      Assert.fail("Wrong type received: " + resultset.get(0).field("customReferences"));
    }

    resultset =
        executeQuery(
            "select customReferences['second']['name'] from Profile where"
                + " customReferences['second']['name'] is not null",
            database);
    Assert.assertEquals(resultset.size(), 1);

    resultset =
        executeQuery(
            "select customReferences['second']['name'] as value from Profile where"
                + " customReferences['second']['name'] is not null",
            database);
    Assert.assertEquals(resultset.size(), 1);

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapNew() {
    Map<String, YTDocument> customReferences = new HashMap<String, YTDocument>();
    customReferences.put("first", new YTDocument("name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new YTDocument("name", "Jay", "surname", "Miner"));

    database.begin();
    YTDocument doc = new YTDocument("Profile");
    doc.field("customReferences", customReferences, YTType.EMBEDDEDMAP);

    doc.save();
    database.commit();

    List<YTDocument> resultset =
        executeQuery(
            "select from Profile where customReferences.keys() CONTAINS 'first'", database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences.values() CONTAINS ( name like 'Ja%')",
            database);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.get(0).getIdentity(), doc.getIdentity());

    database.begin();
    doc.delete();
    database.commit();
  }

  @Test
  public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
    List<YTDocument> result =
        executeQuery(
            "select * from cluster:profile where races contains"
                + " (name.toLowerCase(Locale.ENGLISH).subString(0,1) = 'e')",
            database);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("profile"));
      Assert.assertNotNull(record.field("races"));

      Collection<YTDocument> races = record.field("races");
      boolean found = false;
      for (YTDocument race : races) {
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
    record = new YTDocument();
    record.setClassName("Animal");
    record.field("name", "Cat");

    database.begin();
    Collection<YTDocument> races = new HashSet<YTDocument>();
    races.add(((YTDocument) database.newInstance("AnimalRace")).field("name", "European"));
    races.add(((YTDocument) database.newInstance("AnimalRace")).field("name", "Siamese"));
    record.field("age", 10);
    record.field("races", races);
    record.save();
    database.commit();

    List<YTDocument> result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['European','Asiatic'])",
            database);

    boolean found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (YTDocument race : races) {
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
            database);

    found = false;
    for (int i = 0; i < result.size() && !found; ++i) {
      record = result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.field("races"));

      races = record.field("races");
      for (YTDocument race : races) {
        if (race.field("name").equals("European") || race.field("name").equals("Asiatic")) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result =
        executeQuery(
            "select * from cluster:animal where races contains (name in ['aaa','bbb'])", database);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (name in ['European','Asiatic'])",
            database);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (name in ['European','Siamese'])",
            database);
    Assert.assertEquals(result.size(), 1);

    result =
        executeQuery(
            "select * from cluster:animal where races containsall (age < 100) LIMIT 1000 SKIP 0",
            database);
    Assert.assertEquals(result.size(), 0);

    result =
        executeQuery(
            "select * from cluster:animal where not ( races contains (age < 100) ) LIMIT 20 SKIP 0",
            database);
    Assert.assertEquals(result.size(), 1);

    database.begin();
    record.delete();
    database.commit();
  }

  @Test
  public void queryCollectionInNumbers() {
    record = new YTDocument();
    record.setClassName("Animal");
    record.field("name", "Cat");

    Collection<Integer> rates = new HashSet<Integer>();
    rates.add(100);
    rates.add(200);
    record.field("rates", rates);

    database.begin();
    record.save("animal");
    database.commit();

    List<YTDocument> result =
        executeQuery("select * from cluster:animal where rates contains 500", database);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where rates contains 100", database);
    Assert.assertEquals(result.size(), 1);

    database.begin();
    record.delete();
    database.commit();
  }

  @Test
  public void queryWhereRidDirectMatching() {
    int clusterId = database.getMetadata().getSchema().getClass("ORole").getDefaultClusterId();
    List<Long> positions = getValidPositions(clusterId);

    List<YTDocument> result =
        executeQuery(
            "select * from OUser where roles contains #" + clusterId + ":" + positions.get(0),
            database);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryWhereInpreparred() {
    List<YTDocument> result =
        executeQuery("select * from OUser where name in [ :name ]", database, "admin");

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((YTDocument) result.get(0).getRecord()).field("name"), "admin");
  }

  @Test
  public void queryAllOperator() {
    List<YTDocument> result = executeQuery("select from Account where all() is null", database);

    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void queryOrderBy() {
    List<YTDocument> result = executeQuery("select from Profile order by name", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    boolean isNullSegment = true; // NULL VALUES AT THE BEGINNING!
    for (YTDocument d : result) {
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
      executeQuery("select from Profile order by name aaaa", database);
      Assert.fail();
    } catch (YTCommandSQLParsingException e) {
    }
  }

  @Test
  public void queryLimitOnly() {
    List<YTDocument> result = executeQuery("select from Profile limit 1", database);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void querySkipOnly() {
    List<YTDocument> result = executeQuery("select from Profile", database);
    int total = result.size();

    result = executeQuery("select from Profile skip 1", database);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithSkipAndLimit() {
    List<YTDocument> result = executeQuery("select from Profile", database);

    List<YTDocument> page = executeQuery("select from Profile skip 10 limit 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOffsetOnly() {
    List<YTDocument> result = executeQuery("select from Profile", database);
    int total = result.size();

    result = executeQuery("select from Profile offset 1", database);
    Assert.assertEquals(result.size(), total - 1);
  }

  @Test
  public void queryPaginationWithOffsetAndLimit() {
    List<YTDocument> result = executeQuery("select from Profile", database);

    List<YTDocument> page = executeQuery("select from Profile offset 10 limit 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderBySkipAndLimit() {
    List<YTDocument> result = executeQuery("select from Profile order by name", database);

    List<YTDocument> page =
        executeQuery("select from Profile order by name limit 10 skip 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryPaginationWithOrderByDescSkipAndLimit() {
    List<YTDocument> result = executeQuery("select from Profile order by name desc", database);

    List<YTDocument> page =
        executeQuery("select from Profile order by name desc limit 10 skip 10", database);
    Assert.assertEquals(page.size(), 10);

    for (int i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), (Object) result.get(10 + i));
    }
  }

  @Test
  public void queryOrderByAndLimit() {
    List<YTDocument> result = executeQuery("select from Profile order by name limit 2", database);

    Assert.assertTrue(result.size() <= 2);

    String lastName = null;
    for (YTDocument d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionAndOrderBy() {
    List<YTDocument> result =
        executeQuery("select from Profile where name is not null order by name", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (YTDocument d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) >= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryConditionsAndOrderBy() {
    List<YTDocument> result =
        executeQuery(
            "select from Profile where name is not null order by name desc, id asc", database);

    Assert.assertTrue(result.size() != 0);

    String lastName = null;
    for (YTDocument d : result) {
      if (lastName != null && d.field("name") != null) {
        Assert.assertTrue(((String) d.field("name")).compareTo(lastName) <= 0);
      }
      lastName = d.field("name");
    }
  }

  @Test
  public void queryRecordTargetRid() {
    int profileClusterId =
        database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> positions = getValidPositions(profileClusterId);

    List<YTDocument> result =
        executeQuery("select from " + profileClusterId + ":" + positions.get(0), database);

    Assert.assertEquals(result.size(), 1);

    for (YTDocument d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    }
  }

  @Test
  public void queryRecordTargetRids() {
    int profileClusterId =
        database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> positions = getValidPositions(profileClusterId);

    List<YTDocument> result =
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
            database);

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(
        result.get(0).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(0));
    Assert.assertEquals(
        result.get(1).getIdentity().toString(), "#" + profileClusterId + ":" + positions.get(1));
  }

  @Test
  public void queryRecordAttribRid() {

    int profileClusterId =
        database.getMetadata().getSchema().getClass("Profile").getDefaultClusterId();
    List<Long> postions = getValidPositions(profileClusterId);

    List<YTDocument> result =
        executeQuery(
            "select from Profile where @rid = #" + profileClusterId + ":" + postions.get(0),
            database);

    Assert.assertEquals(result.size(), 1);

    for (YTDocument d : result) {
      Assert.assertEquals(
          d.getIdentity().toString(), "#" + profileClusterId + ":" + postions.get(0));
    }
  }

  @Test
  public void queryRecordAttribClass() {
    List<YTDocument> result = executeQuery("select from Profile where @class = 'Profile'",
        database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertEquals(d.getClassName(), "Profile");
    }
  }

  @Test
  public void queryRecordAttribVersion() {
    List<YTDocument> result = executeQuery("select from Profile where @version > 0", database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertTrue(d.getVersion() > 0);
    }
  }

  @Test
  public void queryRecordAttribSize() {
    List<YTDocument> result = executeQuery("select from Profile where @size >= 50", database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertTrue(d.toStream().length >= 50);
    }
  }

  @Test
  public void queryRecordAttribType() {
    List<YTDocument> result = executeQuery("select from Profile where @type = 'document'",
        database);

    Assert.assertTrue(result.size() != 0);

    for (YTDocument d : result) {
      Assert.assertEquals(ORecordInternal.getRecordType(d), YTDocument.RECORD_TYPE);
    }
  }

  @Test
  public void queryWrongOperator() {
    try {
      executeQuery("select from Profile where name like.toLowerCase() '%Jay%'", database);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void queryEscaping() {
    executeQuery("select from Profile where name like '%\\'Jay%'", database);
  }

  @Test
  public void queryWithLimit() {
    Assert.assertEquals(executeQuery("select from Profile limit 3", database).size(), 3);
  }

  @SuppressWarnings("unused")
  @Test
  public void testRecordNumbers() {
    long tot = database.countClass("V");

    int count = 0;
    for (YTDocument record : database.browseClass("V")) {
      count++;
    }

    Assert.assertEquals(count, tot);

    Assert.assertTrue(executeQuery("select from V", database).size() >= tot);
  }

  @Test
  public void queryWithManualPagination() {
    YTRID last = new ChangeableRecordId();
    List<YTDocument> resultset =
        executeQuery("select from Profile where @rid > ? LIMIT 3", database, last);

    int iterationCount = 0;
    Assert.assertFalse(resultset.isEmpty());
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() < 0
                || (d.getIdentity().getClusterId() >= last.getClusterId())
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = executeQuery("select from Profile where @rid > ? LIMIT 3", database, last);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPagination() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile LIMIT 3");
    YTRID last = new ChangeableRecordId();

    List<YTDocument> resultset = database.query(query);

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationAndRidInWhere() {
    int clusterId = database.getClusterIdByName("profile");

    long[] range = database.getStorage().getClusterDataRange(database, clusterId);

    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>(
            "select from Profile where @rid between #"
                + clusterId
                + ":"
                + range[0]
                + " and #"
                + clusterId
                + ":"
                + range[1]
                + " LIMIT 3");

    YTRID last = new ChangeableRecordId();

    List<YTDocument> resultset = database.query(query);

    Assert.assertEquals(resultset.get(0).getIdentity(), new YTRecordId(clusterId, range[0]));

    int iterationCount = 0;
    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertEquals(last, new YTRecordId(clusterId, range[1]));
    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhere() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile where followers.length() > 0 LIMIT 3");
    YTRID last = new ChangeableRecordId();

    List<YTDocument> resultset = database.query(query);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      // System.out.printf("\nIterating page %d, last record is %s", iterationCount, last);

      iterationCount++;
      resultset = database.query(query);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVar() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile where followers.length() > ? LIMIT 3");
    YTRID last = new ChangeableRecordId();

    List<YTDocument> resultset = database.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAutomaticPaginationWithWhereAndBindingVarAtTheFirstQueryCall() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile where followers.length() > ? LIMIT 3");
    YTRID last = new ChangeableRecordId();

    List<YTDocument> resultset = database.query(query, 0);

    int iterationCount = 0;

    while (!resultset.isEmpty()) {
      Assert.assertTrue(resultset.size() <= 3);

      for (YTDocument d : resultset) {
        Assert.assertTrue(
            d.getIdentity().getClusterId() >= last.getClusterId()
                && d.getIdentity().getClusterPosition() > last.getClusterPosition());
      }

      last = resultset.get(resultset.size() - 1).getIdentity();

      iterationCount++;
      resultset = database.query(query, 0);
    }

    Assert.assertTrue(iterationCount > 1);
  }

  @Test
  public void queryWithAbsenceOfAutomaticPaginationBecauseOfBindingVarReset() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile where followers.length() > ? LIMIT 3");

    List<YTDocument> resultset = database.query(query, -1);

    final YTRID firstRidFirstQuery = resultset.get(0).getIdentity();

    resultset = database.query(query, -2);

    final YTRID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void includeFields() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select expand( roles.include('name') ) from OUser");

    List<YTDocument> resultset = database.query(query);

    for (YTDocument d : resultset) {
      Assert.assertTrue(d.fields() <= 1);
      if (d.fields() == 1) {
        Assert.assertTrue(d.containsField("name"));
      }
    }
  }

  @Test
  public void excludeFields() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select expand( roles.exclude('rules') ) from OUser");

    List<YTDocument> resultset = database.query(query);

    for (YTDocument d : resultset) {
      Assert.assertFalse(d.containsField("rules"));
    }
  }

  @Test
  public void excludeAttributes() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>(
            "select expand( roles.exclude('@rid', '@class') ) from OUser");

    List<YTDocument> resultset = database.query(query);

    for (YTDocument d : resultset) {
      Assert.assertFalse(d.getIdentity().isPersistent());
      Assert.assertNull(d.getSchemaClass());
    }
  }

  @Test
  public void queryResetPagination() {
    final OSQLSynchQuery<YTDocument> query =
        new OSQLSynchQuery<YTDocument>("select from Profile LIMIT 3");

    List<YTDocument> resultset = database.query(query);
    final YTRID firstRidFirstQuery = resultset.get(0).getIdentity();
    query.resetPagination();

    resultset = database.query(query);
    final YTRID firstRidSecondQueryQuery = resultset.get(0).getIdentity();

    Assert.assertEquals(firstRidFirstQuery, firstRidSecondQueryQuery);
  }

  @Test
  public void queryBetween() {
    List<YTDocument> result =
        executeQuery("select * from account where nr between 10 and 20", database);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      Assert.assertTrue(
          ((Integer) record.field("nr")) >= 10 && ((Integer) record.field("nr")) <= 20);
    }
  }

  @Test
  public void queryParenthesisInStrings() {

    database.command("INSERT INTO account (name) VALUES ('test (demo)')").close();

    List<YTDocument> result =
        executeQuery("select * from account where name = 'test (demo)'", database);

    Assert.assertEquals(result.size(), 1);

    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);
      Assert.assertEquals(record.field("name"), "test (demo)");
    }
  }

  @Test
  public void queryMathOperators() {
    List<YTDocument> result = executeQuery("select * from account where id < 3 + 4", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 3 + 4);
    }

    result = executeQuery("select * from account where id < 10 - 3", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 10 - 3);
    }

    result = executeQuery("select * from account where id < 3 * 2", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 3 * 2);
    }

    result = executeQuery("select * from account where id < 120 / 20", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 120 / 20);
    }

    result = executeQuery("select * from account where id < 27 % 10", database);
    Assert.assertFalse(result.isEmpty());
    for (int i = 0; i < result.size(); ++i) {
      Assert.assertTrue(((Number) result.get(i).field("id")).intValue() < 27 % 10);
    }

    result = executeQuery("select * from account where id = id * 1", database);
    Assert.assertFalse(result.isEmpty());

    List<YTDocument> result2 =
        executeQuery("select count(*) as tot from account where id >= 0", database);
    Assert.assertEquals(result.size(), ((Number) result2.get(0).field("tot")).intValue());
  }

  @Test
  public void testBetweenWithParameters() {

    final List<YTDocument> result =
        executeQuery(
            "select * from company where id between ? and ? and salary is not null",
            database,
            4,
            7);

    System.out.println("testBetweenWithParameters:");
    for (YTDocument d : result) {
      System.out.println(d);
    }

    Assert.assertEquals(result.size(), 4, "Found: " + result);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final YTDocument record : result) {
      resultsList.remove(record.<Integer>field("id"));
    }
  }

  @Test
  public void testInWithParameters() {

    final List<YTDocument> result =
        executeQuery(
            "select * from company where id in [?, ?, ?, ?] and salary is not null",
            database,
            4,
            5,
            6,
            7);

    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<Integer>(Arrays.asList(4, 5, 6, 7));
    for (final YTDocument record : result) {
      resultsList.remove(record.<Integer>field("id"));
    }
  }

  @Test
  public void testEqualsNamedParameter() {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<YTDocument> result =
        executeQuery(
            "select * from company where id = :id and salary is not null", database, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testQueryAsClass() {

    List<YTDocument> result =
        executeQuery("select from Account where addresses.@class in [ 'Address' ]", database);
    Assert.assertFalse(result.isEmpty());
    for (YTDocument d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((YTDocument)
              ((Collection<YTIdentifiable>) d.field("addresses")).iterator().next().getRecord())
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  @Test
  public void testQueryNotOperator() {

    List<YTDocument> result =
        executeQuery(
            "select from Account where not ( addresses.@class in [ 'Address' ] )", database);
    Assert.assertFalse(result.isEmpty());
    for (YTDocument d : result) {
      Assert.assertTrue(
          d.field("addresses") == null
              || ((Collection<YTIdentifiable>) d.field("addresses")).isEmpty()
              || !((YTDocument)
              ((Collection<YTIdentifiable>) d.field("addresses"))
                  .iterator()
                  .next()
                  .getRecord())
              .getSchemaClass()
              .getName()
              .equals("Address"));
    }
  }

  @Test
  public void testSquareBracketsOnCondition() {
    List<YTDocument> result =
        executeQuery(
            "select from Account where addresses[@class='Address'][city.country.name] ="
                + " 'Washington'",
            database);
    Assert.assertFalse(result.isEmpty());
    for (YTDocument d : result) {
      Assert.assertNotNull(d.field("addresses"));
      Assert.assertEquals(
          ((YTDocument)
              ((Collection<YTIdentifiable>) d.field("addresses")).iterator().next().getRecord())
              .getSchemaClass()
              .getName(),
          "Address");
    }
  }

  public void testParams() {
    YTClass test = database.getMetadata().getSchema().getClass("test");
    if (test == null) {
      test = database.getMetadata().getSchema().createClass("test");
      test.createProperty(database, "f1", YTType.STRING);
      test.createProperty(database, "f2", YTType.STRING);
    }
    YTDocument document = new YTDocument(test);
    document.field("f1", "a").field("f2", "a");

    database.begin();
    database.save(document);
    database.commit();

    Map<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("p1", "a");
    database.query(new OSQLSynchQuery<YTDocument>("select from test where (f1 = :p1)"), parameters);
    database.query(
        new OSQLSynchQuery<YTDocument>("select from test where f1 = :p1 and f2 = :p1"), parameters);
  }

  @Test
  public void queryInstanceOfOperator() {
    List<YTDocument> result = executeQuery("select from Account", database);

    Assert.assertTrue(result.size() != 0);

    List<YTDocument> result2 =
        executeQuery("select from Account where @this instanceof 'Account'", database);

    Assert.assertEquals(result2.size(), result.size());

    List<YTDocument> result3 =
        executeQuery("select from Account where @class instanceof 'Account'", database);

    Assert.assertEquals(result3.size(), result.size());
  }

  @Test
  public void subQuery() {
    List<YTDocument> result =
        executeQuery(
            "select from Account where name in ( select name from Account where name is not null"
                + " limit 1 )",
            database);

    Assert.assertTrue(result.size() != 0);
  }

  @Test
  public void subQueryNoFrom() {
    List<YTDocument> result2 =
        executeQuery(
            "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
                + " addresses.size() > 0 )",
            database);

    Assert.assertTrue(result2.size() != 0);
    Assert.assertTrue(result2.get(0).field("$names") instanceof Collection<?>);
    Assert.assertFalse(((Collection<?>) result2.get(0).field("$names")).isEmpty());
  }

  @Test
  public void subQueryLetAndIndexedWhere() {
    List<YTDocument> result =
        executeQuery("select $now from OUser let $now = eval('42') where name = 'admin'", database);

    Assert.assertEquals(result.size(), 1);
    Assert.assertNotNull(result.get(0).field("$now"), result.get(0).toString());
  }

  @Test
  public void queryOrderByWithLimit() {

    YTSchema schema = database.getMetadata().getSchema();
    YTClass facClass = schema.getClass("FicheAppelCDI");
    if (facClass == null) {
      facClass = schema.createClass("FicheAppelCDI");
    }
    if (!facClass.existsProperty("date")) {
      facClass.createProperty(database, "date", YTType.DATE);
    }

    final Calendar currentYear = Calendar.getInstance();
    final Calendar oneYearAgo = Calendar.getInstance();
    oneYearAgo.add(Calendar.YEAR, -1);

    database.begin();
    YTDocument doc1 = new YTDocument(facClass);
    doc1.field("context", "test");
    doc1.field("date", currentYear.getTime());
    doc1.save();

    YTDocument doc2 = new YTDocument(facClass);
    doc2.field("context", "test");
    doc2.field("date", oneYearAgo.getTime());
    doc2.save();
    database.commit();

    List<YTDocument> result =
        database.query(
            new OSQLSynchQuery<YTDocument>(
                "select * from " + facClass.getName() + " where context = 'test' order by date",
                1));

    Calendar smaller = Calendar.getInstance();
    smaller.setTime(result.get(0).field("date", Date.class));
    Assert.assertEquals(smaller.get(Calendar.YEAR), oneYearAgo.get(Calendar.YEAR));

    result =
        database.query(
            new OSQLSynchQuery<YTDocument>(
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
    int clusterId = database.getClusterIdByName("profile");

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

    List<YTDocument> resultset =
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
            database,
            new YTRecordId(clusterId, minPos));

    Assert.assertEquals(resultset.size(), 1);

    Assert.assertEquals(resultset.get(0).field("oid"),
        new YTRecordId(clusterId, maxPos).toString());
  }

  @Test
  public void testSelectFromListParameter() {
    YTClass placeClass = database.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(database, "id", YTType.STRING);
    placeClass.createProperty(database, "descr", YTType.STRING);
    placeClass.createIndex(database, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    YTDocument odoc = new YTDocument("Place");
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");

    database.begin();
    database.save(odoc);
    database.commit();

    odoc = new YTDocument("Place");
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");

    database.begin();
    database.save(odoc);
    database.commit();

    Map<String, Object> params = new HashMap<String, Object>();
    List<String> inputValues = new ArrayList<String>();
    inputValues.add("lago_di_como");
    inputValues.add("lecco");
    params.put("place", inputValues);

    List<YTDocument> result = executeQuery("select from place where id in :place", database,
        params);
    Assert.assertEquals(1, result.size());

    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidFromListParameter() {
    YTClass placeClass = database.getMetadata().getSchema().createClass("Place", 1);
    placeClass.createProperty(database, "id", YTType.STRING);
    placeClass.createProperty(database, "descr", YTType.STRING);
    placeClass.createIndex(database, "place_id_index", INDEX_TYPE.UNIQUE, "id");

    List<YTRID> inputValues = new ArrayList<YTRID>();

    YTDocument odoc = new YTDocument("Place");
    odoc.field("id", "adda");
    odoc.field("descr", "Adda");

    database.begin();
    database.save(odoc);
    database.commit();

    inputValues.add(odoc.getIdentity());

    odoc = new YTDocument("Place");
    odoc.field("id", "lago_di_como");
    odoc.field("descr", "Lago di Como");

    database.begin();
    database.save(odoc);
    database.commit();
    inputValues.add(odoc.getIdentity());

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("place", inputValues);

    List<YTDocument> result =
        executeQuery("select from place where @rid in :place", database, params);
    Assert.assertEquals(2, result.size());

    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testSelectRidInList() {
    YTClass placeClass = database.getMetadata().getSchema().createClass("Place", 1);
    database.getMetadata().getSchema().createClass("FamousPlace", 1, placeClass);

    YTDocument firstPlace = new YTDocument("Place");

    database.begin();
    database.save(firstPlace);
    YTDocument secondPlace = new YTDocument("Place");
    database.save(secondPlace);
    YTDocument famousPlace = new YTDocument("FamousPlace");
    database.save(famousPlace);
    database.commit();

    YTRID secondPlaceId = secondPlace.getIdentity();
    YTRID famousPlaceId = famousPlace.getIdentity();
    // if one of these two asserts fails, the test will be meaningless.
    Assert.assertTrue(secondPlaceId.getClusterId() < famousPlaceId.getClusterId());
    Assert.assertTrue(secondPlaceId.getClusterPosition() > famousPlaceId.getClusterPosition());

    List<YTDocument> result =
        executeQuery(
            "select from Place where @rid in [" + secondPlaceId + "," + famousPlaceId + "]",
            database);
    Assert.assertEquals(2, result.size());

    database.getMetadata().getSchema().dropClass("FamousPlace");
    database.getMetadata().getSchema().dropClass("Place");
  }

  @Test
  public void testMapKeys() {
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("id", 4);
    final List<YTDocument> result =
        executeQuery(
            "select * from company where id = :id and salary is not null", database, params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAsynch() {
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<YTDocument> synchResultOne =
        database.command(new OSQLSynchQuery<YTDocument>(sqlOne)).execute(database);
    final List<YTDocument> synchResultTwo =
        database.command(new OSQLSynchQuery<YTDocument>(sqlTwo)).execute(database);

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<YTDocument> asynchResultOne = new ArrayList<YTDocument>();
    final List<YTDocument> asynchResultTwo = new ArrayList<YTDocument>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

    database
        .command(
            new OSQLAsynchQuery<YTDocument>(
                sqlOne,
                new OCommandResultListener() {
                  @Override
                  public boolean result(YTDatabaseSessionInternal querySession, Object iRecord) {
                    asynchResultOne.add((YTDocument) iRecord);
                    return true;
                  }

                  @Override
                  public void end() {
                    endOneCalled.set(true);

                    database
                        .command(
                            new OSQLAsynchQuery<YTDocument>(
                                sqlTwo,
                                new OCommandResultListener() {
                                  @Override
                                  public boolean result(YTDatabaseSessionInternal querySession,
                                      Object iRecord) {
                                    asynchResultTwo.add((YTDocument) iRecord);
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
                        .execute(database);
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }))
        .execute(database);

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(
        ODocumentHelper.compareCollections(
            database, synchResultTwo, database, asynchResultTwo, null),
        "synchResultTwo=" + synchResultTwo.size() + " asynchResultTwo=" + asynchResultTwo.size());
    Assert.assertTrue(
        ODocumentHelper.compareCollections(
            database, synchResultOne, database, asynchResultOne, null),
        "synchResultOne=" + synchResultOne.size() + " asynchResultOne=" + asynchResultOne.size());
  }

  @Test
  public void queryAsynchHalfForheFirstQuery() {
    final String sqlOne = "select * from company where id between 4 and 7";
    final String sqlTwo =
        "select $names let $names = (select EXPAND( addresses.city ) as city from Account where"
            + " addresses.size() > 0 )";

    final List<YTDocument> synchResultOne =
        database.command(new OSQLSynchQuery<YTDocument>(sqlOne)).execute(database);
    final List<YTDocument> synchResultTwo =
        database.command(new OSQLSynchQuery<YTDocument>(sqlTwo)).execute(database);

    Assert.assertTrue(synchResultOne.size() > 0);
    Assert.assertTrue(synchResultTwo.size() > 0);

    final List<YTDocument> asynchResultOne = new ArrayList<YTDocument>();
    final List<YTDocument> asynchResultTwo = new ArrayList<YTDocument>();
    final AtomicBoolean endOneCalled = new AtomicBoolean();
    final AtomicBoolean endTwoCalled = new AtomicBoolean();

    database
        .command(
            new OSQLAsynchQuery<YTDocument>(
                sqlOne,
                new OCommandResultListener() {
                  @Override
                  public boolean result(YTDatabaseSessionInternal querySession, Object iRecord) {
                    asynchResultOne.add((YTDocument) iRecord);
                    return asynchResultOne.size() < synchResultOne.size() / 2;
                  }

                  @Override
                  public void end() {
                    endOneCalled.set(true);

                    database
                        .command(
                            new OSQLAsynchQuery<YTDocument>(
                                sqlTwo,
                                new OCommandResultListener() {
                                  @Override
                                  public boolean result(YTDatabaseSessionInternal querySession,
                                      Object iRecord) {
                                    asynchResultTwo.add((YTDocument) iRecord);
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
                        .execute(database);
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }))
        .execute(database);

    Assert.assertTrue(endOneCalled.get());
    Assert.assertTrue(endTwoCalled.get());

    Assert.assertTrue(
        ODocumentHelper.compareCollections(
            database,
            synchResultOne.subList(0, synchResultOne.size() / 2),
            database,
            asynchResultOne,
            null));
    Assert.assertTrue(
        ODocumentHelper.compareCollections(
            database, synchResultTwo, database, asynchResultTwo, null));
  }

  @Test
  public void queryOrderByRidDesc() {
    List<YTDocument> result = executeQuery("select from OUser order by @rid desc", database);

    Assert.assertFalse(result.isEmpty());

    YTRID lastRid = null;
    for (YTDocument d : result) {
      YTRID rid = d.getIdentity();

      if (lastRid != null) {
        Assert.assertTrue(rid.compareTo(lastRid) < 0);
      }
      lastRid = rid;
    }

    YTDocument res =
        database.command(new OCommandSQL("explain select from OUser order by @rid desc"))
            .execute(database);
    Assert.assertNull(res.field("orderByElapsed"));
  }

  @Test
  public void testSelectFromIndexValues() {
    database.command("create index selectFromIndexValues on Profile (name) notunique").close();

    final List<YTDocument> classResult =
        new ArrayList<YTDocument>(
            (List<YTDocument>)
                database.query(
                    new OSQLSynchQuery<YTDocument>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<YTDocument> indexValuesResult =
        database.query(
            new OSQLSynchQuery<YTDocument>(
                "select from indexvalues:selectFromIndexValues where ((nick like 'J%') or (nick"
                    + " like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (YTDocument document : indexValuesResult) {
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
    database.command("create index selectFromIndexValuesAsc on Profile (name) notunique").close();

    final List<YTDocument> classResult =
        new ArrayList<YTDocument>(
            (List<YTDocument>)
                database.query(
                    new OSQLSynchQuery<YTDocument>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<YTDocument> indexValuesResult =
        database.query(
            new OSQLSynchQuery<YTDocument>(
                "select from indexvaluesasc:selectFromIndexValuesAsc where ((nick like 'J%') or"
                    + " (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (YTDocument document : indexValuesResult) {
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
    database.command("create index selectFromIndexValuesDesc on Profile (name) notunique").close();

    final List<YTDocument> classResult =
        new ArrayList<YTDocument>(
            (List<YTDocument>)
                database.query(
                    new OSQLSynchQuery<YTDocument>(
                        "select from Profile where ((nick like 'J%') or (nick like 'N%')) and (name"
                            + " is not null)")));

    final List<YTDocument> indexValuesResult =
        database.query(
            new OSQLSynchQuery<YTDocument>(
                "select from indexvaluesdesc:selectFromIndexValuesDesc where ((nick like 'J%') or"
                    + " (nick like 'N%')) and (name is not null)"));

    Assert.assertEquals(indexValuesResult.size(), classResult.size());

    String lastName = null;

    for (YTDocument document : indexValuesResult) {
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
    YTDocument doc = new YTDocument();
    doc.field("test", "test");
    database.query("select from OUser where @rid = ?", doc).close();
    Assert.assertTrue(doc.isDirty());
  }

  public void testQueryLetExecutedOnce() {
    final List<YTIdentifiable> result =
        database.query(
            new OSQLSynchQuery<YTIdentifiable>(
                "select name, $counter as counter from OUser let $counter = eval(\"$counter +"
                    + " 1\")"));

    Assert.assertFalse(result.isEmpty());
    int i = 1;
    for (YTIdentifiable r : result) {
      Assert.assertEquals(((YTDocument) r.getRecord()).<Object>field("counter"), i++);
    }
  }

  @Test
  public void testMultipleClustersWithPagination() throws Exception {
    final YTClass cls = database.getMetadata().getSchema().createClass("PersonMultipleClusters");
    cls.addCluster(database, "PersonMultipleClusters_1");
    cls.addCluster(database, "PersonMultipleClusters_2");
    cls.addCluster(database, "PersonMultipleClusters_3");
    cls.addCluster(database, "PersonMultipleClusters_4");

    try {
      Set<String> names =
          new HashSet<String>(Arrays.asList("Luca", "Jill", "Sara", "Tania", "Gianluca", "Marco"));
      for (String n : names) {
        database.begin();
        new YTDocument("PersonMultipleClusters").field("First", n).save();
        database.commit();
      }

      OSQLSynchQuery<YTDocument> query =
          new OSQLSynchQuery<YTDocument>(
              "select from PersonMultipleClusters where @rid > ? limit 2");
      List<YTDocument> resultset = database.query(query, new ChangeableRecordId());

      while (!resultset.isEmpty()) {
        final YTRID last = resultset.get(resultset.size() - 1).getIdentity();

        for (YTDocument personDoc : resultset) {
          Assert.assertTrue(names.contains(personDoc.field("First")));
          Assert.assertTrue(names.remove(personDoc.field("First")));
        }

        resultset = database.query(query, last);
      }

      Assert.assertTrue(names.isEmpty());

    } finally {
      database.getMetadata().getSchema().dropClass("PersonMultipleClusters");
    }
  }

  @Test
  public void testOutFilterInclude() {
    YTSchema schema = database.getMetadata().getSchema();
    schema.createClass("TestOutFilterInclude", schema.getClass("V"));
    database.command("create class linkedToOutFilterInclude extends E").close();
    database.command("insert into TestOutFilterInclude content { \"name\": \"one\" }").close();
    database.command("insert into TestOutFilterInclude content { \"name\": \"two\" }").close();
    database
        .command(
            "create edge linkedToOutFilterInclude from (select from TestOutFilterInclude where name"
                + " = 'one') to (select from TestOutFilterInclude where name = 'two')")
        .close();

    final List<YTIdentifiable> result =
        database.query(
            new OSQLSynchQuery<YTIdentifiable>(
                "select"
                    + " expand(out('linkedToOutFilterInclude')[@class='TestOutFilterInclude'].include('@rid'))"
                    + " from TestOutFilterInclude where name = 'one'"));

    Assert.assertEquals(result.size(), 1);

    for (YTIdentifiable r : result) {
      Assert.assertNull(((YTDocument) r.getRecord()).field("name"));
    }
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<YTDocument> iteratorCluster =
        database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext()) {
        break;
      }

      YTDocument doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  @Test
  public void testBinaryClusterSelect() {
    database.command("create blob cluster binarycluster").close();
    database.reload();
    YTBlob bytes = new YTRecordBytes(new byte[]{1, 2, 3});

    database.begin();
    database.save(bytes, "binarycluster");
    database.commit();

    List<YTIdentifiable> result =
        database.query(new OSQLSynchQuery<YTIdentifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 1);

    database.command("delete from cluster:binarycluster").close();

    result = database.query(
        new OSQLSynchQuery<YTIdentifiable>("select from cluster:binarycluster"));

    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testExpandSkip() {
    YTSchema schema = database.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    final YTClass cls = schema.createClass("TestExpandSkip", v);
    cls.createProperty(database, "name", YTType.STRING);
    cls.createIndex(database, "TestExpandSkip.name", INDEX_TYPE.UNIQUE, "name");
    database.command("CREATE VERTEX TestExpandSkip set name = '1'").close();
    database.command("CREATE VERTEX TestExpandSkip set name = '2'").close();
    database.command("CREATE VERTEX TestExpandSkip set name = '3'").close();
    database.command("CREATE VERTEX TestExpandSkip set name = '4'").close();

    database
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestExpandSkip WHERE name = '1') to (SELECT FROM"
                + " TestExpandSkip WHERE name <> '1')")
        .close();

    List<YTIdentifiable> result =
        database.query("select expand(out()) from TestExpandSkip where name = '1'").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 3);

    Map<Object, Object> params = new HashMap<Object, Object>();
    params.put("values", Arrays.asList("2", "3", "antani"));
    result =
        database
            .query(
                "select expand(out()[name in :values]) from TestExpandSkip where name = '1'",
                params)
            .stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        database.query("select expand(out()) from TestExpandSkip where name = '1' skip 1").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        database.query("select expand(out()) from TestExpandSkip where name = '1' skip 2").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);

    result =
        database.query("select expand(out()) from TestExpandSkip where name = '1' skip 3").stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 0);

    result =
        database
            .query("select expand(out()) from TestExpandSkip where name = '1' skip 1 limit 1")
            .stream()
            .map((e) -> e.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testPolymorphicEdges() {
    YTSchema schema = database.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass e = schema.getClass("E");
    final YTClass v1 = schema.createClass("TestPolymorphicEdges_V", v);
    final YTClass e1 = schema.createClass("TestPolymorphicEdges_E1", e);
    final YTClass e2 = schema.createClass("TestPolymorphicEdges_E2", e1);

    database.command("CREATE VERTEX TestPolymorphicEdges_V set name = '1'").close();
    database.command("CREATE VERTEX TestPolymorphicEdges_V set name = '2'").close();
    database.command("CREATE VERTEX TestPolymorphicEdges_V set name = '3'").close();

    database
        .command(
            "CREATE EDGE TestPolymorphicEdges_E1 FROM (SELECT FROM TestPolymorphicEdges_V WHERE"
                + " name = '1') to (SELECT FROM TestPolymorphicEdges_V WHERE name = '2')")
        .close();
    database
        .command(
            "CREATE EDGE TestPolymorphicEdges_E2 FROM (SELECT FROM TestPolymorphicEdges_V WHERE"
                + " name = '1') to (SELECT FROM TestPolymorphicEdges_V WHERE name = '3')")
        .close();

    List<YTIdentifiable> result =
        database
            .query(
                "select expand(out('TestPolymorphicEdges_E1')) from TestPolymorphicEdges_V where"
                    + " name = '1'")
            .stream()
            .map((r) -> r.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    result =
        database
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
    YTSchema schema = database.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    final YTClass cls = schema.createClass("TestSizeOfLink", v);
    database.command("CREATE VERTEX TestSizeOfLink set name = '1'").close();
    database.command("CREATE VERTEX TestSizeOfLink set name = '2'").close();
    database.command("CREATE VERTEX TestSizeOfLink set name = '3'").close();
    database
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestSizeOfLink WHERE name = '1') to (SELECT FROM"
                + " TestSizeOfLink WHERE name <> '1')")
        .close();

    List<YTIdentifiable> result =
        database
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
    YTSchema schema = database.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    final YTClass cls = schema.createClass("EmbeddedMapAndDotNotation", v);
    database.command("CREATE VERTEX EmbeddedMapAndDotNotation set name = 'foo'").close();
    database
        .command(
            "CREATE VERTEX EmbeddedMapAndDotNotation set data = {\"bar\": \"baz\", \"quux\":"
                + " 1}, name = 'bar'")
        .close();
    database
        .command(
            "CREATE EDGE E FROM (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'foo') to"
                + " (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'bar')")
        .close();

    List<YTIdentifiable> result =
        database.query(
            new OSQLSynchQuery<YTIdentifiable>(
                " select out().data as result from (select from EmbeddedMapAndDotNotation where"
                    + " name = 'foo')"));
    Assert.assertEquals(result.size(), 1);
    YTDocument doc = result.get(0).getRecord();
    Assert.assertNotNull(doc);
    List list = doc.field("result");
    Assert.assertEquals(list.size(), 1);
    Object first = list.get(0);
    Assert.assertTrue(first instanceof Map);
    Assert.assertEquals(((Map) first).get("bar"), "baz");
  }

  @Test
  public void testLetWithQuotedValue() {
    YTSchema schema = database.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    final YTClass cls = schema.createClass("LetWithQuotedValue", v);
    database.command("CREATE VERTEX LetWithQuotedValue set name = \"\\\"foo\\\"\"").close();

    List<YTIdentifiable> result =
        database.query(
            new OSQLSynchQuery<YTIdentifiable>(
                " select expand($a) let $a = (select from LetWithQuotedValue where name ="
                    + " \"\\\"foo\\\"\")"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testNestedProjection1() {
    String className = this.getClass().getSimpleName() + "_testNestedProjection1";
    database.command("create class " + className).close();

    database.begin();
    YTEntity elem1 = database.newEntity(className);
    elem1.setProperty("name", "a");
    elem1.save();

    YTEntity elem2 = database.newEntity(className);
    elem2.setProperty("name", "b");
    elem2.setProperty("surname", "lkj");
    elem2.save();

    YTEntity elem3 = database.newEntity(className);
    elem3.setProperty("name", "c");
    elem3.save();

    YTEntity elem4 = database.newEntity(className);
    elem4.setProperty("name", "d");
    elem4.setProperty("elem1", elem1);
    elem4.setProperty("elem2", elem2);
    elem4.setProperty("elem3", elem3);
    elem4.save();
    database.commit();

    YTResultSet result =
        database.query(
            "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    Assert.assertTrue(result.hasNext());
    YTResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertTrue(item.getProperty("elem1") instanceof YTResult);
    Assert.assertEquals("a", ((YTResult) item.getProperty("elem1")).getProperty("name"));
    Assert.assertEquals("b", ((YTResult) item.getProperty("elem2")).getProperty("name"));
    Assert.assertNull(((YTResult) item.getProperty("elem2")).getProperty("surname"));
    result.close();
  }

  @Override
  protected List<YTDocument> executeQuery(String sql, YTDatabaseSessionInternal db,
      Object... args) {
    YTResultSet rs = db.query(sql, args);
    List<YTDocument> result = new ArrayList<>();
    while (rs.hasNext()) {
      result.add((YTDocument) rs.next().toEntity());
    }
    rs.close();
    return result;
  }
}
