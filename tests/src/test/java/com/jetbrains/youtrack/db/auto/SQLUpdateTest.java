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
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
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

    var positions = getAddressValidPositions();

    session.begin();
    var records =
        session.command(
            "update Profile set salary = 120.30, location = "
                + positions.get(2)
                + ", salary_cloned = salary where surname = 'Obama'");
    session.commit();

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 3);
  }

  @Test
  public void updateWithWhereRid() {

    var result =
        session.command("select @rid as rid from Profile where surname = 'Obama'").stream()
            .toList();

    Assert.assertEquals(result.size(), 3);

    session.begin();
    var records =
        session.command(
            "update Profile set salary = 133.00 where @rid = ?",
            result.get(0).<Object>getProperty("rid"));
    session.commit();

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 1);
  }

  @Test
  public void updateUpsertOperator() {

    session.begin();
    var result =
        session.command(
            "UPDATE Profile SET surname='Merkel' RETURN AFTER where surname = 'Merkel'");
    session.commit();
    Assert.assertEquals(result.stream().count(), 0);

    session.begin();
    result =
        session.command(
            "UPDATE Profile SET surname='Merkel' UPSERT RETURN AFTER  where surname = 'Merkel'");
    session.commit();

    Assert.assertEquals(result.stream().count(), 1);

    result = session.command("SELECT FROM Profile  where surname = 'Merkel'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test(dependsOnMethods = "updateWithWhereOperator")
  public void updateCollectionsAddWithWhereOperator() {
    session.begin();
    var positions = getAddressValidPositions();
    updatedRecords =
        session
            .command("update Account set addresses = addresses || " + positions.get(0))
            .next()
            .getProperty("count");
    session.commit();
  }

  @Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
  public void updateCollectionsRemoveWithWhereOperator() {
    var positions = getAddressValidPositions();
    session.begin();
    final long records =
        session
            .command("update Account remove addresses = " + positions.get(0))
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertEquals(records, updatedRecords);
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateCollectionsWithSetOperator() {

    var docs = session.query("select from Account").stream().toList();

    var positions = getAddressValidPositions();

    for (var doc : docs) {

      session.begin();
      final long records =
          session
              .command(
                  "update Account set addresses = ["
                      + positions.get(0)
                      + ","
                      + positions.get(1)
                      + ","
                      + positions.get(2)
                      + "] where @rid = "
                      + doc.getIdentity())
              .next()
              .getProperty("count");
      session.commit();

      session.begin();
      Assert.assertEquals(records, 1);

      EntityImpl loadedDoc = session.load(doc.getIdentity());
      Assert.assertEquals(((List<?>) loadedDoc.field("addresses")).size(), 3);
      Assert.assertEquals(
          ((Identifiable) ((List<?>) loadedDoc.field("addresses")).get(0)).getIdentity(),
          positions.get(0));
      loadedDoc.field("addresses", doc.getProperty("addresses"));

      session.bindToSession(loadedDoc);
      session.commit();
    }
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithSetOperator() {

    session.begin();
    var element =
        session
            .command(
                "insert into O (equaledges, name, properties) values ('no',"
                    + " 'circleUpdate', {'round':'eeee', 'blaaa':'zigzag'} )")
            .next()
            .asEntity();

    Assert.assertNotNull(element);

    long records =
        session
            .command(
                "update "
                    + element.getIdentity()
                    + " set properties = {'roundOne':'ffff',"
                    + " 'bla':'zagzig','testTestTEST':'okOkOK'}")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertEquals(records, 1);

    Entity loadedElement = session.load(element.getIdentity());

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

    var total = session.countClass("Profile");

    session.begin();
    Long records = session.command("update Profile set sex = 'male'").next().getProperty("count");
    session.commit();

    Assert.assertEquals(records.intValue(), (int) total);
  }

  @Test(dependsOnMethods = "updateAllOperator")
  public void updateWithWildcards() {

    session.begin();
    long updated =
        session
            .command("update Profile set sex = ? where sex = 'male' limit 1", "male")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertEquals(updated, 1);
  }

  @Test
  public void updateWithWildcardsOnSetAndWhere() {

    session.createClass("Person");
    session.begin();
    var doc = ((EntityImpl) session.newEntity("Person"));
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");

    session.commit();

    checkUpdatedDoc(session, "Torino", "fmale");

    /* THESE COMMANDS ARE OK */
    session.begin();
    session.command("update Person set gender = 'female' where name = 'Raf'", "Raf");
    session.commit();

    checkUpdatedDoc(session, "Torino", "female");

    session.begin();
    session.command("update Person set city = 'Turin' where name = ?", "Raf");
    session.commit();

    checkUpdatedDoc(session, "Turin", "female");

    session.begin();
    session.command("update Person set gender = ? where name = 'Raf'", "F");
    session.commit();

    checkUpdatedDoc(session, "Turin", "F");

    session.begin();
    session.command(
        "update Person set gender = ?, city = ? where name = 'Raf'", "FEMALE", "TORINO");
    session.commit();

    checkUpdatedDoc(session, "TORINO", "FEMALE");

    session.begin();
    session.command("update Person set gender = ? where name = ?", "f", "Raf");
    session.commit();

    checkUpdatedDoc(session, "TORINO", "f");
  }

  public void updateWithReturn() {
    var doc = ((EntityImpl) session.newEntity("Data"));
    session.begin();
    doc.field("name", "Pawel");
    doc.field("city", "Wroclaw");
    doc.field("really_big_field", "BIIIIIIIIIIIIIIIGGGGGGG!!!");

    session.commit();

    // check AFTER
    var sqlString = "UPDATE " + doc.getIdentity() + " SET gender='male' RETURN AFTER";
    session.begin();
    var result1 = session.command(sqlString).stream().toList();
    session.commit();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(result1.get(0).getIdentity(), doc.getIdentity());
    Assert.assertEquals(result1.get(0).getProperty("gender"), "male");

    sqlString =
        "UPDATE " + doc.getIdentity() + " set Age = 101 RETURN AFTER $current.Age";
    session.begin();
    result1 = session.command(sqlString).stream().toList();
    session.commit();

    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).hasProperty("$current.Age"));
    Assert.assertEquals(result1.get(0).<Object>getProperty("$current.Age"), 101);
    // check exclude + WHERE + LIMIT
    sqlString =
        "UPDATE "
            + doc.getIdentity()
            + " set Age = Age + 100 RETURN AFTER $current.Exclude('really_big_field') as res WHERE"
            + " Age=101 LIMIT 1";
    session.begin();
    result1 = session.command(sqlString).stream().toList();
    session.commit();

    Assert.assertEquals(result1.size(), 1);
    var element = result1.get(0).<Result>getProperty("res");
    Assert.assertTrue(element.hasProperty("Age"));
    Assert.assertEquals(element.<Integer>getProperty("Age"), 201);
    Assert.assertFalse(element.hasProperty("really_big_field"));
  }

  @Test
  public void updateWithNamedParameters() {
    var doc = ((EntityImpl) session.newEntity("Data"));

    session.begin();
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");

    session.commit();

    var updatecommand = "update Data set gender = :gender , city = :city where name = :name";
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("gender", "f");
    params.put("city", "TOR");
    params.put("name", "Raf");

    session.begin();
    session.command(updatecommand, params);
    session.commit();

    var result = session.query("select * from Data");
    var oDoc = result.next();
    Assert.assertEquals("Raf", oDoc.getProperty("name"));
    Assert.assertEquals("TOR", oDoc.getProperty("city"));
    Assert.assertEquals("f", oDoc.getProperty("gender"));
    result.close();
  }

  public void updateIncrement() {

    var result1 =
        session.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result1.isEmpty());

    session.begin();
    updatedRecords =
        session
            .command("update Account set salary += 10 where salary is defined")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertTrue(updatedRecords > 0);

    var result2 =
        session.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (var i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary");
      Assert.assertEquals(salary2, salary1 + 10);
    }

    session.begin();
    updatedRecords =
        session
            .command("update Account set salary -= 10 where salary is defined")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertTrue(updatedRecords > 0);

    var result3 =
        session.command("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result3.isEmpty());
    Assert.assertEquals(result3.size(), result1.size());

    for (var i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary3 = result3.get(i).getProperty("salary");
      Assert.assertEquals(salary3, salary1);
    }
  }

  public void updateSetMultipleFields() {

    var result1 =
        session.query("select salary from Account where salary is defined").stream().toList();
    Assert.assertFalse(result1.isEmpty());

    session.begin();
    updatedRecords =
        session
            .command(
                "update Account set salary2 = salary, checkpoint = true where salary is defined")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertTrue(updatedRecords > 0);

    var result2 =
        session.query("select from Account where salary is defined").stream().toList();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (var i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary2");
      Assert.assertEquals(salary2, salary1);
      Assert.assertEquals(result2.get(i).<Object>getProperty("checkpoint"), true);
    }
  }

  public void updateAddMultipleFields() {

    session.begin();
    updatedRecords =
        session
            .command("update Account set myCollection = myCollection || [1,2] limit 1")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertTrue(updatedRecords > 0);

    var result2 =
        session.command("select from Account where myCollection is defined").stream().toList();
    Assert.assertEquals(result2.size(), 1);

    Collection<Object> myCollection = result2.iterator().next().getProperty("myCollection");

    Assert.assertTrue(myCollection.containsAll(Arrays.asList(1, 2)));
  }

  @Test(enabled = false)
  public void testEscaping() {
    final Schema schema = session.getMetadata().getSchema();
    schema.createClass("FormatEscapingTest");

    session.begin();
    var document = ((EntityImpl) session.newEntity("FormatEscapingTest"));

    session.commit();

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = format('aaa \\' bbb') WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa ' bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'ccc \\' eee', test2 = format('aaa \\' bbb')"
                + " WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "ccc ' eee");
    Assert.assertEquals(document.field("test2"), "aaa ' bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\n bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \n bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\r bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \r bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\b bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \b bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\t bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \t bbb");

    session.begin();
    session
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\f bbb' WHERE @rid = "
                + document.getIdentity())
        .close();
    session.commit();

    document = session.bindToSession(document);
    Assert.assertEquals(document.field("test"), "aaa \f bbb");
  }

  public void testUpdateVertexContent() {
    final Schema schema = session.getMetadata().getSchema();
    var vertex = schema.getClass("V");
    schema.createClass("UpdateVertexContent", vertex);

    session.begin();
    final var vOneId = session.command("create vertex UpdateVertexContent").next().getIdentity();
    final var vTwoId = session.command("create vertex UpdateVertexContent").next().getIdentity();

    session.command("create edge from " + vOneId + " to " + vTwoId).close();
    session.command("create edge from " + vOneId + " to " + vTwoId).close();
    session.command("create edge from " + vOneId + " to " + vTwoId).close();
    session.commit();

    var result =
        session
            .query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (var doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    session.begin();
    session
        .command("update UpdateVertexContent content {value : 'val'} where @rid = " + vOneId)
        .close();
    session
        .command("update UpdateVertexContent content {value : 'val'} where @rid =  " + vTwoId)
        .close();
    session.commit();

    result =
        session
            .query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (var doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    result =
        session.query("select from UpdateVertexContent").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (var doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  public void testUpdateEdgeContent() {
    final Schema schema = session.getMetadata().getSchema();
    var vertex = schema.getClass("V");
    var edge = schema.getClass("E");

    schema.createClass("UpdateEdgeContentV", vertex);
    schema.createClass("UpdateEdgeContentE", edge);

    session.begin();
    final var vOneId = session.command("create vertex UpdateEdgeContentV").next().getIdentity();
    final var vTwoId = session.command("create vertex UpdateEdgeContentV").next().getIdentity();

    session.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    session.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    session.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    session.commit();

    var result =
        session.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (var doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    session.begin();
    session.command("update UpdateEdgeContentE content {value : 'val'}").close();
    session.commit();

    result =
        session.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (var doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    result = session.query("select from UpdateEdgeContentE").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 3);
    for (var doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  private void checkUpdatedDoc(
      DatabaseSession database, String expectedCity, String expectedGender) {
    var result = database.query("select * from person");
    var oDoc = result.next();
    Assert.assertEquals("Raf", oDoc.getProperty("name"));
    Assert.assertEquals(expectedCity, oDoc.getProperty("city"));
    Assert.assertEquals(expectedGender, oDoc.getProperty("gender"));
  }

  private List<RID> getAddressValidPositions() {
    final List<RID> positions = new ArrayList<>();

    final var iteratorClass = session.browseClass("Address");

    for (var i = 0; i < 7; i++) {
      if (!iteratorClass.hasNext()) {
        break;
      }
      var doc = iteratorClass.next();
      positions.add(doc.getIdentity());
    }
    return positions;
  }

  public void testMultiplePut() {
    session.begin();
    EntityImpl v = session.newInstance("V");

    session.commit();

    session.begin();
    Long records =
        session
            .command(
                "UPDATE"
                    + v.getIdentity()
                    + " SET embmap[\"test\"] = \"Luca\" ,embmap[\"test2\"]=\"Alex\"")
            .next()
            .getProperty("count");
    session.commit();

    Assert.assertEquals(records.intValue(), 1);

    session.begin();
    v = session.bindToSession(v);
    Assert.assertTrue(v.field("embmap") instanceof Map);
    Assert.assertEquals(((Map) v.field("embmap")).size(), 2);
    session.rollback();
  }

  public void testAutoConversionOfEmbeddededListWithLinkedClass() {
    var c = session.getMetadata().getSchema().getOrCreateClass("TestConvert");
    if (!c.existsProperty(session, "embeddedListWithLinkedClass")) {
      c.createProperty(session,
          "embeddedListWithLinkedClass",
          PropertyType.EMBEDDEDLIST,
          session.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));
    }

    session.begin();
    var id =
        session
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedListWithLinkedClass',"
                    + " embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getIdentity();

    session
        .command(
            "UPDATE "
                + id
                + " set embeddedListWithLinkedClass = embeddedListWithLinkedClass || [{'line1':'123"
                + " Fake Street'}]")
        .close();
    session.commit();

    Entity doc = session.load(id);

    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 2);

    session.begin();
    session
        .command(
            "UPDATE "
                + doc.getIdentity()
                + " set embeddedListWithLinkedClass =  embeddedListWithLinkedClass ||"
                + " [{'line1':'123 Fake Street'}]")
        .close();
    session.commit();

    doc = session.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 3);

    List addr = doc.getProperty("embeddedListWithLinkedClass");
    for (var o : addr) {
      Assert.assertTrue(o instanceof EntityImpl);
      Assert.assertEquals(((EntityImpl) o).getSchemaClassName(), "TestConvertLinkedClass");
    }
  }

  public void testPutListOfMaps() {
    var className = "testPutListOfMaps";
    session.getMetadata().getSchema().createClass(className);

    session.begin();
    session
        .command("insert into " + className + " set list = [{\"xxx\":1},{\"zzz\":3},{\"yyy\":2}]")
        .close();
    session.command("UPDATE " + className + " set list = list || [{\"kkk\":4}]").close();
    session.commit();

    var result =
        session.query("select from " + className).stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
    var doc = result.get(0);
    List list = doc.getProperty("list");
    Assert.assertEquals(list.size(), 4);
    var fourth = list.get(3);

    Assert.assertTrue(fourth instanceof Map);
    Assert.assertEquals(((Map) fourth).keySet().iterator().next(), "kkk");
    Assert.assertEquals(((Map) fourth).values().iterator().next(), 4);
  }
}
