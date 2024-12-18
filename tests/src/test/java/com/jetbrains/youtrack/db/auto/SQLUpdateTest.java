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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test
public class SQLUpdateTest extends BaseDBTest {

  private long updatedRecords;

  @Parameters(value = "remote")
  public SQLUpdateTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    generateProfiles();
    generateCompanyData();
  }

  @Test
  public void updateWithWhereOperator() {

    List<RID> positions = getAddressValidPositions();

    db.begin();
    ResultSet records =
        db.command(
            "update Profile set salary = 120.30, location = "
                + positions.get(2)
                + ", salary_cloned = salary where surname = 'Obama'");
    db.commit();

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 3);
  }

  @Test
  public void updateWithWhereRid() {

    List<Result> result =
        db.command("select @rid as rid from Profile where surname = 'Obama'").stream()
            .toList();

    Assert.assertEquals(result.size(), 3);

    db.begin();
    ResultSet records =
        db.command(
            "update Profile set salary = 133.00 where @rid = ?",
            result.get(0).<Object>getProperty("rid"));
    db.commit();

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 1);
  }

  @Test
  public void updateUpsertOperator() {

    db.begin();
    ResultSet result =
        db.command(
            "UPDATE Profile SET surname='Merkel' RETURN AFTER where surname = 'Merkel'");
    db.commit();
    Assert.assertEquals(result.stream().count(), 0);

    db.begin();
    result =
        db.command(
            "UPDATE Profile SET surname='Merkel' UPSERT RETURN AFTER  where surname = 'Merkel'");
    db.commit();

    Assert.assertEquals(result.stream().count(), 1);

    result = db.command("SELECT FROM Profile  where surname = 'Merkel'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test(dependsOnMethods = "updateWithWhereOperator")
  public void updateCollectionsAddWithWhereOperator() {
    db.begin();
    var positions = getAddressValidPositions();
    updatedRecords =
        db
            .command("update Account set addresses = addresses || " + positions.get(0))
            .next()
            .getProperty("count");
    db.commit();
  }

  @Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
  public void updateCollectionsRemoveWithWhereOperator() {
    var positions = getAddressValidPositions();
    db.begin();
    final long records =
        db
            .command("update Account remove addresses = " + positions.get(0))
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertEquals(records, updatedRecords);
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateCollectionsWithSetOperator() {

    List<Result> docs = db.query("select from Account").stream().toList();

    List<RID> positions = getAddressValidPositions();

    for (Result doc : docs) {

      db.begin();
      final long records =
          db
              .command(
                  "update Account set addresses = ["
                      + positions.get(0)
                      + ","
                      + positions.get(1)
                      + ","
                      + positions.get(2)
                      + "] where @rid = "
                      + doc.getRecordId())
              .next()
              .getProperty("count");
      db.commit();

      db.begin();
      Assert.assertEquals(records, 1);

      EntityImpl loadedDoc = db.load(doc.getRecordId());
      Assert.assertEquals(((List<?>) loadedDoc.field("addresses")).size(), 3);
      Assert.assertEquals(
          ((Identifiable) ((List<?>) loadedDoc.field("addresses")).get(0)).getIdentity(),
          positions.get(0));
      loadedDoc.field("addresses", doc.<Object>getProperty("addresses"));

      db.save(db.bindToSession(loadedDoc));
      db.commit();
    }
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithSetOperator() {

    db.begin();
    Entity element =
        db
            .command(
                "insert into O (equaledges, name, properties) values ('no',"
                    + " 'circleUpdate', {'round':'eeee', 'blaaa':'zigzag'} )")
            .next()
            .toEntity();

    Assert.assertNotNull(element);

    long records =
        db
            .command(
                "update "
                    + element.getIdentity()
                    + " set properties = {'roundOne':'ffff',"
                    + " 'bla':'zagzig','testTestTEST':'okOkOK'}")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertEquals(records, 1);

    Entity loadedElement = db.load(element.getIdentity());

    Assert.assertTrue(loadedElement.getProperty("properties") instanceof Map);

    Map<Object, Object> entries = loadedElement.getProperty("properties");
    Assert.assertEquals(entries.size(), 3);

    Assert.assertNull(entries.get("round"));
    Assert.assertNull(entries.get("blaaa"));

    Assert.assertEquals(entries.get("roundOne"), "ffff");
    Assert.assertEquals(entries.get("bla"), "zagzig");
    Assert.assertEquals(entries.get("testTestTEST"), "okOkOK");
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateAllOperator() {

    long total = db.countClass("Profile");

    db.begin();
    Long records = db.command("update Profile set sex = 'male'").next().getProperty("count");
    db.commit();

    Assert.assertEquals(records.intValue(), (int) total);
  }

  @Test(dependsOnMethods = "updateAllOperator")
  public void updateWithWildcards() {

    db.begin();
    long updated =
        db
            .command("update Profile set sex = ? where sex = 'male' limit 1", "male")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertEquals(updated, 1);
  }

  @Test
  public void updateWithWildcardsOnSetAndWhere() {

    db.createClass("Person");
    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("Person"));
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();
    db.commit();

    checkUpdatedDoc(db, "Torino", "fmale");

    /* THESE COMMANDS ARE OK */
    db.begin();
    db.command("update Person set gender = 'female' where name = 'Raf'", "Raf");
    db.commit();

    checkUpdatedDoc(db, "Torino", "female");

    db.begin();
    db.command("update Person set city = 'Turin' where name = ?", "Raf");
    db.commit();

    checkUpdatedDoc(db, "Turin", "female");

    db.begin();
    db.command("update Person set gender = ? where name = 'Raf'", "F");
    db.commit();

    checkUpdatedDoc(db, "Turin", "F");

    db.begin();
    db.command(
        "update Person set gender = ?, city = ? where name = 'Raf'", "FEMALE", "TORINO");
    db.commit();

    checkUpdatedDoc(db, "TORINO", "FEMALE");

    db.begin();
    db.command("update Person set gender = ? where name = ?", "f", "Raf");
    db.commit();

    checkUpdatedDoc(db, "TORINO", "f");
  }

  public void updateWithReturn() {
    EntityImpl doc = ((EntityImpl) db.newEntity("Data"));
    db.begin();
    doc.field("name", "Pawel");
    doc.field("city", "Wroclaw");
    doc.field("really_big_field", "BIIIIIIIIIIIIIIIGGGGGGG!!!");
    doc.save();
    db.commit();

    // check AFTER
    String sqlString = "UPDATE " + doc.getIdentity().toString() + " SET gender='male' RETURN AFTER";
    db.begin();
    List<Result> result1 = db.command(sqlString).stream().toList();
    db.commit();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(result1.get(0).getRecordId(), doc.getIdentity());
    Assert.assertEquals(result1.get(0).getProperty("gender"), "male");

    sqlString =
        "UPDATE " + doc.getIdentity().toString() + " set Age = 101 RETURN AFTER $current.Age";
    db.begin();
    result1 = db.command(sqlString).stream().toList();
    db.commit();

    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).hasProperty("$current.Age"));
    Assert.assertEquals(result1.get(0).<Object>getProperty("$current.Age"), 101);
    // check exclude + WHERE + LIMIT
    sqlString =
        "UPDATE "
            + doc.getIdentity().toString()
            + " set Age = Age + 100 RETURN AFTER $current.Exclude('really_big_field') as res WHERE"
            + " Age=101 LIMIT 1";
    db.begin();
    result1 = db.command(sqlString).stream().toList();
    db.commit();

    Assert.assertEquals(result1.size(), 1);
    var element = result1.get(0).<Result>getProperty("res");
    Assert.assertTrue(element.hasProperty("Age"));
    Assert.assertEquals(element.<Integer>getProperty("Age"), 201);
    Assert.assertFalse(element.hasProperty("really_big_field"));
  }

  @Test
  public void updateWithNamedParameters() {
    EntityImpl doc = ((EntityImpl) db.newEntity("Data"));

    db.begin();
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();
    db.commit();

    String updatecommand = "update Data set gender = :gender , city = :city where name = :name";
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("gender", "f");
    params.put("city", "TOR");
    params.put("name", "Raf");

    db.begin();
    db.command(updatecommand, params);
    db.commit();

    ResultSet result = db.query("select * from Data");
    Result oDoc = result.next();
    Assert.assertEquals("Raf", oDoc.getProperty("name"));
    Assert.assertEquals("TOR", oDoc.getProperty("city"));
    Assert.assertEquals("f", oDoc.getProperty("gender"));
    result.close();
  }

  public void updateIncrement() {

    List<Result> result1 =
        db.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result1.isEmpty());

    db.begin();
    updatedRecords =
        db
            .command("update Account set salary += 10 where salary is defined")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertTrue(updatedRecords > 0);

    List<Result> result2 =
        db.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary");
      Assert.assertEquals(salary2, salary1 + 10);
    }

    db.begin();
    updatedRecords =
        db
            .command("update Account set salary -= 10 where salary is defined")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertTrue(updatedRecords > 0);

    List<Result> result3 =
        db.command("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result3.isEmpty());
    Assert.assertEquals(result3.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary3 = result3.get(i).getProperty("salary");
      Assert.assertEquals(salary3, salary1);
    }
  }

  public void updateSetMultipleFields() {

    List<Result> result1 =
        db.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result1.isEmpty());

    db.begin();
    updatedRecords =
        db
            .command(
                "update Account set salary2 = salary, checkpoint = true where salary is defined")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertTrue(updatedRecords > 0);

    List<Result> result2 =
        db.query("select from Account where salary is defined").stream().toList();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary2");
      Assert.assertEquals(salary2, salary1);
      Assert.assertEquals(result2.get(i).<Object>getProperty("checkpoint"), true);
    }
  }

  public void updateAddMultipleFields() {

    db.begin();
    updatedRecords =
        db
            .command("update Account set myCollection = myCollection || [1,2] limit 1")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertTrue(updatedRecords > 0);

    List<Result> result2 =
        db.command("select from Account where myCollection is defined").stream().toList();
    Assert.assertEquals(result2.size(), 1);

    Collection<Object> myCollection = result2.iterator().next().getProperty("myCollection");

    Assert.assertTrue(myCollection.containsAll(Arrays.asList(1, 2)));
  }

  @Test(enabled = false)
  public void testEscaping() {
    final Schema schema = db.getMetadata().getSchema();
    schema.createClass("FormatEscapingTest");

    db.begin();
    EntityImpl document = ((EntityImpl) db.newEntity("FormatEscapingTest"));
    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = format('aaa \\' bbb') WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa ' bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'ccc \\' eee', test2 = format('aaa \\' bbb')"
                + " WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "ccc ' eee");
    Assert.assertEquals(document.field("test2"), "aaa ' bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\n bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \n bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\r bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \r bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\b bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \b bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\t bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \t bbb");

    db.begin();
    db
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\f bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    db.commit();

    document = db.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \f bbb");
  }

  public void testUpdateVertexContent() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass vertex = schema.getClass("V");
    schema.createClass("UpdateVertexContent", vertex);

    db.begin();
    final RID vOneId = db.command("create vertex UpdateVertexContent").next().getRecordId();
    final RID vTwoId = db.command("create vertex UpdateVertexContent").next().getRecordId();

    db.command("create edge from " + vOneId + " to " + vTwoId).close();
    db.command("create edge from " + vOneId + " to " + vTwoId).close();
    db.command("create edge from " + vOneId + " to " + vTwoId).close();
    db.commit();

    List<Result> result =
        db
            .query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (Result doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    db.begin();
    db
        .command("update UpdateVertexContent content {value : 'val'} where @rid = " + vOneId)
        .close();
    db
        .command("update UpdateVertexContent content {value : 'val'} where @rid =  " + vTwoId)
        .close();
    db.commit();

    result =
        db
            .query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (Result doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    result =
        db.query("select from UpdateVertexContent").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (Result doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  public void testUpdateEdgeContent() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass vertex = schema.getClass("V");
    SchemaClass edge = schema.getClass("E");

    schema.createClass("UpdateEdgeContentV", vertex);
    schema.createClass("UpdateEdgeContentE", edge);

    db.begin();
    final RID vOneId = db.command("create vertex UpdateEdgeContentV").next().getRecordId();
    final RID vTwoId = db.command("create vertex UpdateEdgeContentV").next().getRecordId();

    db.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    db.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    db.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    db.commit();

    List<Result> result =
        db.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (Result doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    db.begin();
    db.command("update UpdateEdgeContentE content {value : 'val'}").close();
    db.commit();

    result =
        db.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (Result doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    result = db.query("select from UpdateEdgeContentE").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 3);
    for (Result doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  private void checkUpdatedDoc(
      DatabaseSession database, String expectedCity, String expectedGender) {
    ResultSet result = database.query("select * from person");
    Result oDoc = result.next();
    Assert.assertEquals("Raf", oDoc.getProperty("name"));
    Assert.assertEquals(expectedCity, oDoc.getProperty("city"));
    Assert.assertEquals(expectedGender, oDoc.getProperty("gender"));
  }

  private List<RID> getAddressValidPositions() {
    final List<RID> positions = new ArrayList<>();

    final var iteratorClass = db.browseClass("Address");

    for (int i = 0; i < 7; i++) {
      if (!iteratorClass.hasNext()) {
        break;
      }
      EntityImpl doc = iteratorClass.next();
      positions.add(doc.getIdentity());
    }
    return positions;
  }

  public void testMultiplePut() {
    db.begin();
    EntityImpl v = db.newInstance("V");
    v.save();
    db.commit();

    db.begin();
    Long records =
        db
            .command(
                "UPDATE"
                    + v.getIdentity()
                    + " SET embmap[\"test\"] = \"Luca\" ,embmap[\"test2\"]=\"Alex\"")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertEquals(records.intValue(), 1);

    db.begin();
    v = db.bindToSession(v);
    Assert.assertTrue(v.field("embmap") instanceof Map);
    Assert.assertEquals(((Map) v.field("embmap")).size(), 2);
    db.rollback();
  }

  public void testAutoConversionOfEmbeddededListWithLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    if (!c.existsProperty("embeddedListWithLinkedClass")) {
      c.createProperty(db,
          "embeddedListWithLinkedClass",
          PropertyType.EMBEDDEDLIST,
          db.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));
    }

    db.begin();
    RID id =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedListWithLinkedClass',"
                    + " embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getRecordId();

    db
        .command(
            "UPDATE "
                + id
                + " set embeddedListWithLinkedClass = embeddedListWithLinkedClass || [{'line1':'123"
                + " Fake Street'}]")
        .close();
    db.commit();

    Entity doc = db.load(id);

    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 2);

    db.begin();
    db
        .command(
            "UPDATE "
                + doc.getIdentity()
                + " set embeddedListWithLinkedClass =  embeddedListWithLinkedClass ||"
                + " [{'line1':'123 Fake Street'}]")
        .close();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 3);

    List addr = doc.getProperty("embeddedListWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof EntityImpl);
      Assert.assertEquals(((EntityImpl) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  public void testPutListOfMaps() {
    String className = "testPutListOfMaps";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    db
        .command("insert into " + className + " set list = [{\"xxx\":1},{\"zzz\":3},{\"yyy\":2}]")
        .close();
    db.command("UPDATE " + className + " set list = list || [{\"kkk\":4}]").close();
    db.commit();

    List<Result> result =
        db.query("select from " + className).stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
    Result doc = result.get(0);
    List list = doc.getProperty("list");
    Assert.assertEquals(list.size(), 4);
    Object fourth = list.get(3);

    Assert.assertTrue(fourth instanceof Map);
    Assert.assertEquals(((Map) fourth).keySet().iterator().next(), "kkk");
    Assert.assertEquals(((Map) fourth).values().iterator().next(), 4);
  }
}
