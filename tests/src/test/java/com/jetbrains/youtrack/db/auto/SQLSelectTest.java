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
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
@SuppressWarnings("unchecked")
public class SQLSelectTest extends AbstractSelectTest {

  @Parameters(value = "remote")
  public SQLSelectTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void init() {
    generateCompanyData();

    addBarackObamaAndFollowers();
    generateProfiles();
  }

  @Test
  public void queryNoDirtyResultset() {
    var result = executeQuery("select from Profile ", db);
    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void queryNoWhere() {
    var result = executeQuery("select from Profile ", db);

    Assert.assertFalse(result.isEmpty());
  }

  @Test
  public void queryParentesisAsRight() {
    var result =
        executeQuery(
            "select from Profile where (name = 'Giuseppe' and ( name <> 'Napoleone' and nick is"
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
    var result = executeQuery("select count(*) from V");
    Assert.assertEquals(result.getFirst().<Object>getProperty("count(*)"), vertexesCount);
  }

  @Test
  public void querySchemaAndLike() {
    var result1 =
        executeQuery("select * from cluster:profile where name like 'Gi%'", db);

    for (var record : result1) {
      Assert.assertTrue(record.asEntity().getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.getProperty("name").toString().startsWith("Gi"));
    }

    var result2 =
        executeQuery("select * from cluster:profile where name like '%epp%'", db);

    Assert.assertEquals(result1, result2);

    var result3 =
        executeQuery("select * from cluster:profile where name like 'Gius%pe'", db);

    Assert.assertEquals(result1, result3);

    result1 = executeQuery("select * from cluster:profile where name like '%Gi%'", db);

    for (var record : result1) {
      Assert.assertTrue(record.asEntity().getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.getProperty("name").toString().contains("Gi"));
    }

    result1 = executeQuery("select * from cluster:profile where name like ?", db, "%Gi%");

    for (var record : result1) {
      Assert.assertTrue(record.asEntity().getClassName().equalsIgnoreCase("profile"));
      Assert.assertTrue(record.getProperty("name").toString().contains("Gi"));
    }
  }

  @Test
  public void queryContainsInEmbeddedSet() {
    Set<String> tags = new HashSet<>();
    tags.add("smart");
    tags.add("nice");

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.setProperty("tags", tags, PropertyType.EMBEDDEDSET);

    db.begin();
    doc.save();
    db.commit();

    var resultset =
        executeQuery("select from Profile where tags CONTAINS 'smart'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    db.begin();
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedList() {
    List<String> tags = new ArrayList<>();
    tags.add("smart");
    tags.add("nice");

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.setProperty("tags", tags);

    db.begin();
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
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentSet() {
    db.begin();
    var coll = new HashSet<EntityImpl>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("coll", coll, PropertyType.EMBEDDEDSET);

    doc.save();
    db.commit();

    var resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertTrue(resultset.getFirst().getProperty("value") instanceof List<?>);
    Assert.assertEquals(
        ((List<EntityImpl>) resultset.getFirst().getProperty("value")).getFirst()
            .getProperty("name"),
        "Jay");

    db.begin();
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryContainsInDocumentList() {
    List<EntityImpl> coll = new ArrayList<>();
    coll.add(new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    coll.add(new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.setProperty("coll", coll, PropertyType.EMBEDDEDLIST);

    db.begin();
    doc.save();
    db.commit();

    var resultset =
        executeQuery(
            "select coll[name='Jay'] as value from Profile where coll is not null", db);
    Assert.assertEquals(resultset.size(), 1);
    Assert.assertTrue(resultset.getFirst().getProperty("value") instanceof List<?>);
    Assert.assertEquals(
        ((EntityImpl) ((List<?>) resultset.getFirst().getProperty("value")).getFirst()).getProperty(
            "name"),
        "Jay");

    db.begin();
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapClassic() {
    Map<String, EntityImpl> customReferences = new HashMap<>();
    customReferences.put("first", new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    db.begin();
    doc.save();
    db.commit();

    var resultset =
        executeQuery("select from Profile where customReferences CONTAINSKEY 'first'", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    resultset =
        executeQuery(
            "select from Profile where customReferences CONTAINSVALUE (name like 'Ja%')", db);

    Assert.assertEquals(resultset.size(), 1);
    Assert.assertEquals(resultset.getFirst().getIdentity().orElseThrow(), doc.getIdentity());

    db.begin();
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryContainsInEmbeddedMapNew() {
    Map<String, EntityImpl> customReferences = new HashMap<>();
    customReferences.put("first", new EntityImpl(db, "name", "Luca", "surname", "Garulli"));
    customReferences.put("second", new EntityImpl(db, "name", "Jay", "surname", "Miner"));

    var doc = ((EntityImpl) db.newEntity("Profile"));
    doc.field("customReferences", customReferences, PropertyType.EMBEDDEDMAP);

    db.begin();
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
    db.bindToSession(doc).delete();
    db.commit();
  }

  @Test
  public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
    var result =
        executeQuery(
            "select * from cluster:profile where races contains"
                + " (name.toLowerCase(Locale.ENGLISH).subString(0,1) = 'e')",
            db);

    for (var record : result) {
      Assert.assertTrue(record.asEntity().getClassName().equalsIgnoreCase("profile"));
      Assert.assertNotNull(record.getProperty("races"));

      Collection<EntityImpl> races = record.getProperty("races");
      var found = false;
      for (var race : races) {
        if (((String) race.getProperty("name")).toLowerCase(Locale.ENGLISH).charAt(0) == 'e') {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  @Test
  public void queryCollectionContainsInRecords() {
    var record = ((EntityImpl) db.newEntity("Animal"));
    record.setProperty("name", "Cat");

    db.begin();
    Collection<Identifiable> races = new HashSet<>();
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

      Assert.assertTrue(Objects.requireNonNull(record.getClassName()).equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.getProperty("races"));

      races = record.getProperty("races");
      for (var race : races) {
        if (Objects.equals(race.getEntity(db).getProperty("name"), "European")
            || Objects.equals(race.getEntity(db).getProperty("name"), "Asiatic")) {
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

      Assert.assertTrue(Objects.requireNonNull(record.getClassName()).equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.getProperty("races"));

      races = record.getProperty("races");
      for (var race : races) {
        if (Objects.equals(race.getEntity(db).getProperty("name"), "European")
            || Objects.equals(race.getEntity(db).getProperty("name"), "Asiatic")) {
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
    db.bindToSession(record).delete();
    db.commit();
  }

  @Test
  public void queryCollectionInNumbers() {
    var record = ((EntityImpl) db.newEntity("Animal"));
    record.field("name", "Cat");

    Collection<Integer> rates = new HashSet<>();
    rates.add(100);
    rates.add(200);
    record.field("rates", rates);

    db.begin();
    record.save("animal");
    db.commit();

    var result = executeQuery(
        "select * from cluster:animal where rates in [100,200]");

    var found = false;
    for (var i = 0; i < result.size() && !found; ++i) {
      record = (EntityImpl) result.get(i).asEntity();

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.getProperty("rates"));

      rates = record.getProperty("rates");
      for (var rate : rates) {
        if (rate == 100 || rate == 105) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where rates in [200,10333]");

    found = false;
    for (var i = 0; i < result.size() && !found; ++i) {
      record = (EntityImpl) result.get(i);

      Assert.assertTrue(record.getClassName().equalsIgnoreCase("animal"));
      Assert.assertNotNull(record.getProperty("rates"));

      rates = record.getProperty("rates");
      for (var rate : rates) {
        if (rate == 100 || rate == 105) {
          found = true;
          break;
        }
      }
    }
    Assert.assertTrue(found);

    result = executeQuery("select * from cluster:animal where rates contains 500", db);
    Assert.assertEquals(result.size(), 0);

    result = executeQuery("select * from cluster:animal where rates contains 100", db);
    Assert.assertEquals(result.size(), 1);

    db.begin();
    db.bindToSession(record).delete();
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
    Assert.assertEquals(result.getFirst().asEntity().getProperty("name"), "admin");
  }

  @Test
  public void queryInAsParameter() {
    var roles = executeQuery("select from orole limit 1", db);

    var result = executeQuery("select * from OUser where roles in ?", db,
        roles);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void queryAnyOperator() {
    var result = executeQuery("select from Profile where any() like 'N%'", db);

    Assert.assertFalse(result.isEmpty());

    for (var record : result) {
      Assert.assertTrue(record.asEntity().getClassName().equalsIgnoreCase("Profile"));

      var found = false;
      for (var fieldValue :
          record.getPropertyNames().stream().map(record::getProperty).toArray()) {
        if (fieldValue != null && !fieldValue.toString().isEmpty()
            && fieldValue.toString().toLowerCase(Locale.ROOT).charAt(0) == 'n') {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
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
    var result = executeQuery("select from Profile order by name desc");

    var page = executeQuery(
        "select from Profile order by name desc limit 10 skip 10");
    Assert.assertEquals(page.size(), 10);

    for (var i = 0; i < page.size(); ++i) {
      Assert.assertEquals(page.get(i), result.get(10 + i));
    }
  }

  @Test
  public void queryOrderByAndLimit() {
    var result = executeQuery("select from Profile order by name limit 2");

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
        executeQuery("select from Profile where name is not null order by name");

    Assert.assertFalse(result.isEmpty());

    String lastName = null;
    for (var d : result) {
      if (lastName != null && d.getProperty("name") != null) {
        Assert.assertTrue(((String) d.getProperty("name")).compareTo(lastName) >= 0);
      }
      lastName = d.getProperty("name");
    }
  }

  @Test(enabled = false)
  public void queryConditionsAndOrderBy() {
    var result =
        executeQuery("select from Profile where name is not null order by name desc, id asc");

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
        executeQuery("select from " + profileClusterId + ":" + positions.getFirst());

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
    var result = executeQuery("select from Profile where @class = 'Profile'");

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
      executeQuery(
          "select from Profile where name like.toLowerCase4(Locale.ENGLISH) '%Jay%'", db);
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
  public void includeFields() {
    var query = "select expand( roles.include('name') ) from OUser";

    var resultset = executeQuery(query);

    for (var d : resultset) {
      Assert.assertTrue(d.getPropertyNames().size() <= 1);
      if (d.getPropertyNames().size() == 1) {
        Assert.assertTrue(d.hasProperty("name"));
      }
    }
  }

  @Test
  public void excludeFields() {
    var query = "select expand( roles.exclude('rules') ) from OUser";

    var resultset = executeQuery(query);

    for (var d : resultset) {
      Assert.assertFalse(d.hasProperty("rules"));
      Assert.assertTrue(d.hasProperty("name"));
    }
  }

  @Test
  public void queryBetween() {
    var result = executeQuery("select * from account where nr between 10 and 20");

    for (var record : result) {
      Assert.assertTrue(
          ((Integer) record.getProperty("nr")) >= 10 && ((Integer) record.getProperty("nr")) <= 20);
    }
  }

  @Test
  public void queryParenthesisInStrings() {

    db.begin();
    db.command("INSERT INTO account (name) VALUES ('test (demo)')");
    db.commit();

    var result = executeQuery("select * from account where name = 'test (demo)'");

    Assert.assertEquals(result.size(), 1);

    for (var record : result) {
      Assert.assertEquals(record.getProperty("name"), "test (demo)");
    }
  }

  @Test
  public void queryMathOperators() {
    var result = executeQuery("select * from account where id < 3 + 4");
    Assert.assertFalse(result.isEmpty());
    for (var document : result) {
      Assert.assertTrue(((Number) document.getProperty("id")).intValue() < 3 + 4);
    }

    result = executeQuery("select * from account where id < 10 - 3");
    Assert.assertFalse(result.isEmpty());
    for (var document : result) {
      Assert.assertTrue(((Number) document.getProperty("id")).intValue() < 10 - 3);
    }

    result = executeQuery("select * from account where id < 3 * 2");
    Assert.assertFalse(result.isEmpty());
    for (var document : result) {
      Assert.assertTrue(((Number) document.getProperty("id")).intValue() < 3 << 1);
    }

    result = executeQuery("select * from account where id < 120 / 20");
    Assert.assertFalse(result.isEmpty());
    for (var document : result) {
      Assert.assertTrue(((Number) document.getProperty("id")).intValue() < 120 / 20);
    }

    result = executeQuery("select * from account where id < 27 % 10");
    Assert.assertFalse(result.isEmpty());
    for (var document : result) {
      Assert.assertTrue(((Number) document.getProperty("id")).intValue() < 27 % 10);
    }

    result = executeQuery("select * from account where id = id * 1");
    Assert.assertFalse(result.isEmpty());

    var result2 = executeQuery("select count(*) as tot from account where id >= 0");
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

    final List<Integer> resultsList = new ArrayList<>(Arrays.asList(4, 5, 6, 7));
    for (var record : result) {
      Assert.assertTrue(resultsList.remove(record.<Integer>getProperty("id")));
    }
  }

  @Test
  public void testInWithParameters() {

    final var result =
        executeQuery(
            "select * from Company where id in [?, ?, ?, ?] and salary is not null",
            db,
            4,
            5,
            6,
            7);

    Assert.assertEquals(result.size(), 4);

    final List<Integer> resultsList = new ArrayList<>(Arrays.asList(4, 5, 6, 7));
    for (var record : result) {
      Assert.assertTrue(resultsList.remove(record.<Integer>getProperty("id")));
    }
  }

  @Test
  public void testEqualsNamedParameter() {

    Map<String, Object> params = new HashMap<>();
    params.put("id", 4);
    final var result =
        executeQuery("select * from Company where id = :id and salary is not null", params);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testQueryAsClass() {

    var result =
        executeQuery("select from Account where addresses.@class in [ 'Address' ]");
    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("addresses"));
      Assert.assertEquals(
          Objects.requireNonNull(
                  ((EntityImpl)
                      ((Collection<Identifiable>) d.getProperty("addresses"))
                          .iterator()
                          .next()
                          .getRecord(db))
                      .getSchemaClass())
              .getName(),
          "Address");
    }
  }

  @Test
  public void testQueryNotOperator() {

    var result =
        executeQuery("select from Account where not ( addresses.@class in [ 'Address' ] )");
    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertTrue(
          d.getProperty("addresses") == null
              || ((Collection<Identifiable>) d.getProperty("addresses")).isEmpty()
              || !Objects.requireNonNull(
                  ((EntityImpl)
                      ((Collection<Identifiable>) d.getProperty("addresses"))
                          .iterator()
                          .next()
                          .getRecord(db))
                      .getSchemaClass())
              .getName()
              .equals("Address"));
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

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("p1", "a");
    executeQuery("select from test where (f1 = :p1)", parameters);
    executeQuery("select from test where f1 = :p1 and f2 = :p1", parameters);
  }

  @Test
  public void queryInstanceOfOperator() {
    var result = executeQuery("select from Account");

    Assert.assertFalse(result.isEmpty());

    var result2 = executeQuery(
        "select from Account where @this instanceof 'Account'");

    Assert.assertEquals(result2.size(), result.size());

    var result3 = executeQuery(
        "select from Account where @class instanceof 'Account'");

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
                + " addresses.size() > 0 )");

    Assert.assertFalse(result2.isEmpty());
    Assert.assertTrue(result2.getFirst().getProperty("$names") instanceof Collection<?>);
    Assert.assertFalse(((Collection<?>) result2.getFirst().getProperty("$names")).isEmpty());
  }

  @Test
  public void subQueryLetAndIndexedWhere() {
    var result =
        executeQuery("select $now from OUser let $now = eval('42') where name = 'admin'");

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
        executeQuery(
            "select * from " + facClass.getName() + " where context = 'test' order by date", 1);

    var smaller = Calendar.getInstance();
    smaller.setTime(result.getFirst().asEntity().getProperty("date"));
    Assert.assertEquals(smaller.get(Calendar.YEAR), oneYearAgo.get(Calendar.YEAR));

    result =
        executeQuery(
            "select * from " + facClass.getName() + " where context = 'test' order by date DESC",
            1);

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

    Map<String, Object> params = new HashMap<>();
    List<String> inputValues = new ArrayList<>();
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

    List<RID> inputValues = new ArrayList<>();

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

    Map<String, Object> params = new HashMap<>();
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
    db.commit();

    var secondPlace = ((EntityImpl) db.newEntity("Place"));

    db.begin();
    db.save(secondPlace);
    db.commit();

    var famousPlace = ((EntityImpl) db.newEntity("FamousPlace"));

    db.begin();
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
    Map<String, Object> params = new HashMap<>();
    params.put("id", 4);

    final var result =
        executeQuery(
            "select * from company where id = :id and salary is not null", db, params);

    Assert.assertEquals(result.size(), 1);
  }

  public void queryOrderByRidDesc() {
    var result = executeQuery("select from OUser order by @rid desc", db);

    Assert.assertFalse(result.isEmpty());

    RID lastRid = null;
    for (var d : result) {
      var rid = d.getIdentity().orElseThrow();
      if (lastRid != null) {
        Assert.assertTrue(rid.compareTo(lastRid) < 0);
      }
      lastRid = rid;
    }

    var res = executeQuery("explain select from OUser order by @rid desc").getFirst();
    Assert.assertNull(res.getProperty("orderByElapsed"));
  }

  public void testQueryParameterNotPersistent() {
    var doc = ((EntityImpl) db.newEntity());
    doc.field("test", "test");
    executeQuery("select from OUser where @rid = ?", doc);
    Assert.assertTrue(doc.isDirty());
  }

  public void testQueryLetExecutedOnce() {
    final var result =
        executeQuery(
            "select name, $counter as counter from OUser let $counter = eval(\"$counter +"
                + " 1\")");

    Assert.assertFalse(result.isEmpty());
    var i = 1;
    for (var r : result) {
      Assert.assertEquals(((EntityImpl) r.asEntity()).<Object>field("counter"), 1);
    }
  }

  @Test
  public void testMultipleClustersWithPagination() {
    final var cls = db.getMetadata().getSchema()
        .createClass("PersonMultipleClusters");
    cls.addCluster(db, "PersonMultipleClusters_1");
    cls.addCluster(db, "PersonMultipleClusters_2");
    cls.addCluster(db, "PersonMultipleClusters_3");
    cls.addCluster(db, "PersonMultipleClusters_4");

    try {
      Set<String> names =
          new HashSet<>(Arrays.asList("Luca", "Jill", "Sara", "Tania", "Gianluca", "Marco"));
      for (var n : names) {
        db.begin();
        ((EntityImpl) db.newEntity("PersonMultipleClusters")).field("First", n).save();
        db.commit();
      }

      var query = "select from PersonMultipleClusters where @rid > ? limit 2";
      var resultset = executeQuery(query, new ChangeableRecordId());

      while (!resultset.isEmpty()) {
        final var last = resultset.getLast().getIdentity().orElseThrow();

        for (var personDoc : resultset) {
          Assert.assertTrue(names.contains(personDoc.<String>getProperty("First")));
          Assert.assertTrue(names.remove(personDoc.<String>getProperty("First")));
        }

        resultset = executeQuery(query, last);
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

    db.begin();
    db.command("insert into TestOutFilterInclude content { \"name\": \"one\" }").close();
    db.command("insert into TestOutFilterInclude content { \"name\": \"two\" }").close();
    db
        .command(
            "create edge linkedToOutFilterInclude from (select from TestOutFilterInclude where name"
                + " = 'one') to (select from TestOutFilterInclude where name = 'two')")
        .close();
    db.commit();

    final var result =
        executeQuery(
            "select"
                + " expand(out('linkedToOutFilterInclude')[@class='TestOutFilterInclude'].include('@rid'))"
                + " from TestOutFilterInclude where name = 'one'");

    Assert.assertEquals(result.size(), 1);

    for (var r : result) {
      Assert.assertNull(r.asEntity().getProperty("name"));
    }
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<>();

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

    var result = db.query("select from cluster:binarycluster");

    Assert.assertEquals(result.stream().count(), 1);

    db.begin();
    db.command("delete from cluster:binarycluster").close();
    db.commit();

    result = db.query("select from cluster:binarycluster");

    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testExpandSkip() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    final var cls = schema.createClass("TestExpandSkip", v);
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createIndex(db, "TestExpandSkip.name", INDEX_TYPE.UNIQUE, "name");

    db.begin();
    db.command("CREATE VERTEX TestExpandSkip set name = '1'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '2'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '3'").close();
    db.command("CREATE VERTEX TestExpandSkip set name = '4'").close();

    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestExpandSkip WHERE name = '1') to (SELECT FROM"
                + " TestExpandSkip WHERE name <> '1')")
        .close();
    db.commit();

    var result = db.query(
        "select expand(out()) from TestExpandSkip where name = '1'");

    Assert.assertEquals(result.stream().count(), 3);

    Map<Object, Object> params = new HashMap<>();
    params.put("values", Arrays.asList("2", "3", "antani"));
    result =
        db.query(
            "select expand(out()[name in :values]) from TestExpandSkip where name = '1'", params);
    Assert.assertEquals(result.stream().count(), 2);

    result = db.query("select expand(out()) from TestExpandSkip where name = '1' skip 1");

    Assert.assertEquals(result.stream().count(), 2);

    result = db.query("select expand(out()) from TestExpandSkip where name = '1' skip 2");
    Assert.assertEquals(result.stream().count(), 1);

    result = db.query("select expand(out()) from TestExpandSkip where name = '1' skip 3");
    Assert.assertEquals(result.stream().count(), 0);

    result =
        db.query("select expand(out()) from TestExpandSkip where name = '1' skip 1 limit 1");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test
  public void testPolymorphicEdges() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var e = schema.getClass("E");
    schema.createClass("TestPolymorphicEdges_V", v);
    final var e1 = schema.createClass("TestPolymorphicEdges_E1", e);
    schema.createClass("TestPolymorphicEdges_E2", e1);

    db.begin();
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
    db.commit();

    var result =
        db.query(
            "select expand(out('TestPolymorphicEdges_E1')) from TestPolymorphicEdges_V where name ="
                + " '1'");
    Assert.assertEquals(result.stream().count(), 2);

    result =
        db.query(
            "select expand(out('TestPolymorphicEdges_E2')) from TestPolymorphicEdges_V where name ="
                + " '1' ");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test
  public void testSizeOfLink() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    schema.createClass("TestSizeOfLink", v);

    db.begin();
    db.command("CREATE VERTEX TestSizeOfLink set name = '1'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '2'").close();
    db.command("CREATE VERTEX TestSizeOfLink set name = '3'").close();
    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM TestSizeOfLink WHERE name = '1') to (SELECT FROM"
                + " TestSizeOfLink WHERE name <> '1')")
        .close();
    db.commit();

    var result =
        db.query(
            " select from (select from TestSizeOfLink where name = '1') where out()[name=2].size()"
                + " > 0");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test
  public void testEmbeddedMapAndDotNotation() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    schema.createClass("EmbeddedMapAndDotNotation", v);

    db.begin();
    db.command("CREATE VERTEX EmbeddedMapAndDotNotation set name = 'foo'").close();
    db
        .command(
            "CREATE VERTEX EmbeddedMapAndDotNotation set data = {\"bar\": \"baz\", \"quux\": 1},"
                + " name = 'bar'")
        .close();
    db
        .command(
            "CREATE EDGE E FROM (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'foo') to"
                + " (SELECT FROM EmbeddedMapAndDotNotation WHERE name = 'bar')")
        .close();
    db.commit();

    var result =
        executeQuery(
            " select out().data as result from (select from EmbeddedMapAndDotNotation where"
                + " name = 'foo')");
    Assert.assertEquals(result.size(), 1);
    var doc = result.getFirst();
    Assert.assertNotNull(doc);
    @SuppressWarnings("rawtypes")
    List list = doc.getProperty("result");
    Assert.assertEquals(list.size(), 1);
    var first = list.getFirst();
    Assert.assertTrue(first instanceof Map);
    //noinspection rawtypes
    Assert.assertEquals(((Map) first).get("bar"), "baz");
  }

  @Test
  public void testLetWithQuotedValue() {
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    schema.createClass("LetWithQuotedValue", v);
    db.begin();
    db.command("CREATE VERTEX LetWithQuotedValue set name = \"\\\"foo\\\"\"").close();
    db.commit();

    var result =
        db.query(
            " select expand($a) let $a = (select from LetWithQuotedValue where name ="
                + " \"\\\"foo\\\"\")");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test
  public void testNamedParams() {
    // issue #7236

    db.command("create class testNamedParams extends V").close();
    db.command("create class testNamedParams_permission extends V").close();
    db.command("create class testNamedParams_HasPermission extends E").close();

    db.begin();
    db.command("insert into testNamedParams_permission set type = ['USER']").close();
    db.command("insert into testNamedParams set login = 20").close();
    db
        .command(
            "CREATE EDGE testNamedParams_HasPermission from (select from testNamedParams) to"
                + " (select from testNamedParams_permission)")
        .close();
    db.commit();

    Map<String, Object> params = new HashMap<>();
    params.put("key", 10);
    params.put("permissions", new String[]{"USER"});
    params.put("limit", 1);
    var results =
        executeQuery(
            "SELECT *, out('testNamedParams_HasPermission').type as permissions FROM"
                + " testNamedParams WHERE login >= :key AND"
                + " out('testNamedParams_HasPermission').type IN :permissions ORDER BY login"
                + " ASC LIMIT :limit",
            params);
    Assert.assertEquals(results.size(), 1);
  }

  @Test
  public void selectLikeFromSet() {
    var vertexClass = "SetContainer";
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var clazz = schema.createClass(vertexClass, v);
    db.begin();
    var container1 = db.newVertex(clazz);
    container1.setProperty("data", Set.of("hello", "world", "baobab"));
    container1.save();
    var container2 = db.newVertex(vertexClass);
    container2.setProperty("data", Set.of("1hello", "2world", "baobab"));
    container2.save();
    db.commit();

    var results = executeQuery("SELECT FROM SetContainer WHERE data LIKE 'wor%'");
    Assert.assertEquals(results.size(), 1);

    results = executeQuery("SELECT FROM SetContainer WHERE data LIKE 'bobo%'");
    Assert.assertEquals(results.size(), 0);

    results = executeQuery("SELECT FROM SetContainer WHERE data LIKE '%hell%'");
    Assert.assertEquals(results.size(), 2);
  }

  @Test
  public void selectLikeFromList() {
    var vertexClass = "ListContainer";
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var clazz = schema.createClass(vertexClass, v);
    db.begin();
    var container1 = db.newVertex(clazz);
    container1.setProperty("data", List.of("hello", "world", "baobab"));
    container1.save();
    var container2 = db.newVertex(vertexClass);
    container2.setProperty("data", List.of("1hello", "2world", "baobab"));
    container2.save();
    db.commit();
    var results = executeQuery("SELECT FROM ListContainer WHERE data LIKE 'wor%'");
    Assert.assertEquals(results.size(), 1);

    results = executeQuery("SELECT FROM ListContainer WHERE data LIKE 'bobo%'");
    Assert.assertEquals(results.size(), 0);

    results = executeQuery("SELECT FROM ListContainer WHERE data LIKE '%hell%'");
    Assert.assertEquals(results.size(), 2);
  }

  @Test
  public void selectLikeFromArray() {
    var vertexClass = "ArrayContainer";
    Schema schema = db.getMetadata().getSchema();
    var v = schema.getClass("V");
    var clazz = schema.createClass(vertexClass, v);
    db.begin();
    var container1 = db.newVertex(clazz);
    container1.setProperty("data", new String[]{"hello", "world", "baobab"});
    container1.save();
    var container2 = db.newVertex(vertexClass);
    container2.setProperty("data", new String[]{"1hello", "2world", "baobab"});
    container2.save();
    db.commit();
    var results = executeQuery("SELECT FROM ArrayContainer WHERE data LIKE 'wor%'");
    Assert.assertEquals(results.size(), 1);

    results = executeQuery("SELECT FROM ArrayContainer WHERE data LIKE 'bobo%'");
    Assert.assertEquals(results.size(), 0);

    results = executeQuery("SELECT FROM ArrayContainer WHERE data LIKE '%hell%'");
    Assert.assertEquals(results.size(), 2);
  }
}
