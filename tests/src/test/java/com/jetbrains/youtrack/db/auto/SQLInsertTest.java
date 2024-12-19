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

import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test
public class SQLInsertTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLInsertTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void insertOperator() {
    if (!db.getMetadata().getSchema().existsClass("Account")) {
      db.getMetadata().getSchema().createClass("Account");
    }

    final int clId = db.addCluster("anotherdefault");
    final SchemaClass profileClass = db.getMetadata().getSchema().getClass("Account");
    profileClass.addClusterId(db, clId);

    if (!db.getMetadata().getSchema().existsClass("Address")) {
      db.getMetadata().getSchema().createClass("Address");
    }

    int addressId = db.getMetadata().getSchema().getClass("Address").getClusterIds()[0];

    for (int i = 0; i < 30; i++) {
      db.begin();
      db.newEntity("Address").save();
      db.commit();
    }
    List<Long> positions = getValidPositions(addressId);

    if (!db.getMetadata().getSchema().existsClass("Profile")) {
      db.getMetadata().getSchema().createClass("Profile");
    }

    db.begin();
    Entity doc =
        db
            .command(
                "insert into Profile (name, surname, salary, location, dummy) values"
                    + " ('Luca','Smith', 109.9, #"
                    + addressId
                    + ":"
                    + positions.get(3)
                    + ", 'hooray')")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getProperty("name"), "Luca");
    Assert.assertEquals(doc.getProperty("surname"), "Smith");
    Assert.assertEquals(((Number) doc.getProperty("salary")).floatValue(), 109.9f);
    Assert.assertEquals(doc.getProperty("location"), new RecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.getProperty("dummy"), "hooray");

    db.begin();
    doc =
        db
            .command(
                "insert into Profile SET name = 'Luca', surname = 'Smith', salary = 109.9,"
                    + " location = #"
                    + addressId
                    + ":"
                    + positions.get(3)
                    + ", dummy =  'hooray'")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getProperty("name"), "Luca");
    Assert.assertEquals(doc.getProperty("surname"), "Smith");
    Assert.assertEquals(((Number) doc.getProperty("salary")).floatValue(), 109.9f);
    Assert.assertEquals(
        ((Identifiable) doc.getProperty("location")).getIdentity(),
        new RecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.getProperty("dummy"), "hooray");
  }

  @Test
  public void insertWithWildcards() {
    int addressId = db.getMetadata().getSchema().getClass("Address").getClusterIds()[0];

    List<Long> positions = getValidPositions(addressId);

    db.begin();
    Entity doc =
        db
            .command(
                "insert into Profile (name, surname, salary, location, dummy) values"
                    + " (?,?,?,?,?)",
                "Marc",
                "Smith",
                120.0,
                new RecordId(addressId, positions.get(3)),
                "hooray")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getProperty("name"), "Marc");
    Assert.assertEquals(doc.getProperty("surname"), "Smith");
    Assert.assertEquals(((Number) doc.getProperty("salary")).floatValue(), 120.0f);
    Assert.assertEquals(doc.getProperty("location"), new RecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.getProperty("dummy"), "hooray");

    db.begin();
    db.delete(db.bindToSession(doc));
    db.commit();

    db.begin();
    doc =
        db
            .command(
                "insert into Profile SET name = ?, surname = ?, salary = ?, location = ?,"
                    + " dummy = ?",
                "Marc",
                "Smith",
                120.0,
                new RecordId(addressId, positions.get(3)),
                "hooray")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getProperty("name"), "Marc");
    Assert.assertEquals(doc.getProperty("surname"), "Smith");
    Assert.assertEquals(((Number) doc.getProperty("salary")).floatValue(), 120.0f);
    Assert.assertEquals(
        ((Identifiable) doc.getProperty("location")).getIdentity(),
        new RecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.getProperty("dummy"), "hooray");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertMap() {
    db.begin();
    Entity doc =
        db
            .command(
                "insert into O (equaledges, name, properties) values ('no',"
                    + " 'circle', {'round':'eeee', 'blaaa':'zigzag'} )")
            .next()
            .getEntity()
            .get();
    db.commit();

    Assert.assertNotNull(doc);

    doc = db.bindToSession(doc);
    Assert.assertEquals(doc.getProperty("equaledges"), "no");
    Assert.assertEquals(doc.getProperty("name"), "circle");
    Assert.assertTrue(doc.getProperty("properties") instanceof Map);

    Map<Object, Object> entries = doc.getProperty("properties");
    Assert.assertEquals(entries.size(), 2);

    Assert.assertEquals(entries.get("round"), "eeee");
    Assert.assertEquals(entries.get("blaaa"), "zigzag");

    db.begin();
    db.delete(db.bindToSession(doc));
    db.commit();

    db.begin();
    doc =
        db
            .command(
                "insert into O SET equaledges = 'no', name = 'circle',"
                    + " properties = {'round':'eeee', 'blaaa':'zigzag'} ")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);

    Assert.assertEquals(doc.getProperty("equaledges"), "no");
    Assert.assertEquals(doc.getProperty("name"), "circle");
    Assert.assertTrue(doc.getProperty("properties") instanceof Map);

    entries = doc.getProperty("properties");
    Assert.assertEquals(entries.size(), 2);

    Assert.assertEquals(entries.get("round"), "eeee");
    Assert.assertEquals(entries.get("blaaa"), "zigzag");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertList() {
    db.begin();
    Entity doc =
        db
            .command(
                "insert into O (equaledges, name, list) values ('yes',"
                    + " 'square', ['bottom', 'top','left','right'] )")
            .next()
            .getEntity()
            .get();
    db.commit();

    Assert.assertNotNull(doc);

    doc = db.bindToSession(doc);
    Assert.assertEquals(doc.getProperty("equaledges"), "yes");
    Assert.assertEquals(doc.getProperty("name"), "square");
    Assert.assertTrue(doc.getProperty("list") instanceof List);

    List<Object> entries = doc.getProperty("list");
    Assert.assertEquals(entries.size(), 4);

    Assert.assertEquals(entries.get(0), "bottom");
    Assert.assertEquals(entries.get(1), "top");
    Assert.assertEquals(entries.get(2), "left");
    Assert.assertEquals(entries.get(3), "right");

    db.begin();
    db.delete(db.bindToSession(doc));
    db.commit();

    db.begin();
    doc =
        db
            .command(
                "insert into O SET equaledges = 'yes', name = 'square', list"
                    + " = ['bottom', 'top','left','right'] ")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);

    Assert.assertEquals(doc.getProperty("equaledges"), "yes");
    Assert.assertEquals(doc.getProperty("name"), "square");
    Assert.assertTrue(doc.getProperty("list") instanceof List);

    entries = doc.getProperty("list");
    Assert.assertEquals(entries.size(), 4);

    Assert.assertEquals(entries.get(0), "bottom");
    Assert.assertEquals(entries.get(1), "top");
    Assert.assertEquals(entries.get(2), "left");
    Assert.assertEquals(entries.get(3), "right");
  }

  @Test
  public void insertWithNoSpaces() {
    db.begin();
    ResultSet res =
        db.command("insert into O (id, title)values(10, 'NoSQL movement')");
    db.commit();

    Assert.assertTrue(res.hasNext());
  }

  @Test
  public void insertAvoidingSubQuery() {
    final Schema schema = db.getMetadata().getSchema();
    if (schema.getClass("test") == null) {
      schema.createClass("test");
    }

    db.begin();
    Result doc = db.command("INSERT INTO test(text) VALUES ('(Hello World)')").next();
    db.commit();

    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getProperty("text"), "(Hello World)");
  }

  @Test
  public void insertSubQuery() {
    final Schema schema = db.getMetadata().getSchema();
    if (schema.getClass("test") == null) {
      schema.createClass("test");
    }

    final ResultSet usersCount = db.query("select count(*) as count from OUser");
    final long uCount = usersCount.next().getProperty("count");
    usersCount.close();

    db.begin();
    Entity doc =
        db
            .command("INSERT INTO test SET names = (select name from OUser)")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertNotNull(doc);
    Assert.assertNotNull(doc.getProperty("names"));
    Assert.assertTrue(doc.getProperty("names") instanceof Collection);
    Assert.assertEquals(((Collection<?>) doc.getProperty("names")).size(), uCount);
  }

  @Test(dependsOnMethods = "insertOperator")
  public void insertCluster() {
    db.begin();
    var doc =
        db
            .command(
                "insert into Account cluster anotherdefault (id, title) values (10, 'NoSQL"
                    + " movement')").stream().findFirst().orElseThrow().toEntity();
    db.commit();

    Assert.assertNotNull(doc);
    Assert.assertEquals(
        doc.getIdentity().getClusterId(), db.getClusterIdByName("anotherdefault"));
    Assert.assertEquals(doc.getClassName(), "Account");
  }

  public void updateMultipleFields() {
    if (!db.getMetadata().getSchema().existsClass("Account")) {
      db.getMetadata().getSchema().createClass("Account");
    }

    db.begin();
    for (int i = 0; i < 30; i++) {
      db.command("insert into O set name = 'foo" + i + "'");
    }
    db.commit();

    List<Long> positions = getValidPositions(3);

    db.begin();
    Identifiable result =
        db
            .command(
                "  INSERT INTO Account SET id= 3232,name= 'my name',map="
                    + " {\"key\":\"value\"},dir= '',user= #3:"
                    + positions.get(0))
            .next()
            .getEntity()
            .get();
    db.commit();
    Assert.assertNotNull(result);

    EntityImpl record = result.getRecord(db);

    record = db.bindToSession(record);
    Assert.assertEquals(record.<Object>field("id"), 3232);
    Assert.assertEquals(record.field("name"), "my name");
    Map<String, String> map = record.field("map");
    Assert.assertEquals(map.get("key"), "value");
    Assert.assertEquals(record.field("dir"), "");
    Assert.assertEquals(record.field("user"), new RecordId(3, positions.get(0)));
  }

  @Test
  public void insertSelect() {
    db.command("CREATE CLASS UserCopy").close();

    db.begin();
    long inserted =
        db
            .command("INSERT INTO UserCopy FROM select from ouser where name <> 'admin' limit 2")
            .stream()
            .count();
    db.commit();

    Assert.assertEquals(inserted, 2);

    List<Result> result =
        db.query("select from UserCopy").toList();

    Assert.assertEquals(result.size(), 2);
    for (var r : result) {
      Assert.assertEquals(r.asEntity().getClassName(), "UserCopy");
      Assert.assertNotEquals(((EntityImpl) r.asEntity()).field("name"), "admin");
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void insertSelectFromProjection() {
    db.command("CREATE CLASS ProjectedInsert").close();
    db.command("CREATE property ProjectedInsert.a Integer (max 3)").close();

    db.begin();
    db.command("INSERT INTO ProjectedInsert FROM select 10 as a ").close();
    db.commit();
  }

  @Test
  @Ignore
  public void insertWithReturn() {
    if (!db.getMetadata().getSchema().existsClass("actor2")) {
      db.command("CREATE CLASS Actor2").close();
    }

    // RETURN with $current.
    EntityImpl doc =
        db
            .command(new CommandSQL("INSERT INTO Actor2 SET FirstName=\"FFFF\" RETURN $current"))
            .execute(db);
    Assert.assertNotNull(doc);
    Assert.assertEquals(doc.getClassName(), "Actor2");
    // RETURN with @rid
    try (ResultSet resultSet1 =
        db.command("INSERT INTO Actor2 SET FirstName=\"Butch 1\" RETURN @rid")) {
      Object res1 = resultSet1.next().getProperty("@rid");
      Assert.assertTrue(res1 instanceof RecordId);
      Assert.assertTrue(((RecordId) ((Identifiable) res1).getIdentity()).isValid());
      // Create many records and return @rid
      try (ResultSet resultSet2 =
          db.command(
              "INSERT INTO Actor2(FirstName,LastName) VALUES"
                  + " ('Jay','Miner'),('Frank','Hermier'),('Emily','Saut')  RETURN @rid")) {

        Object res2 = resultSet2.next().getProperty("@rid");
        Assert.assertTrue(res2 instanceof RecordId);

        // Create many records by INSERT INTO ...FROM and return wrapped field
        RID another = ((Identifiable) res1).getIdentity();
        final String sql =
            "INSERT INTO Actor2 RETURN $current.FirstName  FROM SELECT * FROM ["
                + doc.getIdentity().toString()
                + ","
                + another.toString()
                + "]";
        List res3 = db.command(new CommandSQL(sql)).execute(db);
        Assert.assertEquals(res3.size(), 2);
        Assert.assertTrue(((List<?>) res3).get(0) instanceof EntityImpl);
        final EntityImpl res3doc = (EntityImpl) res3.get(0);
        Assert.assertTrue(res3doc.containsField("result"));
        Assert.assertTrue(
            "FFFF".equalsIgnoreCase(res3doc.field("result"))
                || "Butch 1".equalsIgnoreCase(res3doc.field("result")));
        Assert.assertTrue(res3doc.containsField("rid"));
        Assert.assertTrue(res3doc.containsField("version"));
      }
    }

    // create record using content keyword and update it in sql batch passing recordID between
    // commands
    final String sql2 =
        "let var1 = (INSERT INTO Actor2 CONTENT {Name:\"content\"} RETURN $current.@rid) "
            + "; let var2 = (UPDATE $var1 SET Bingo=1 RETURN AFTER @rid) "
            + " return $var2";
    try (var resSql2ResultSet = db.command(sql2)) {
      var res_sql2 = resSql2ResultSet.next().getProperty("$var2");
      Assert.assertTrue(res_sql2 instanceof RecordId);

      // create record using content keyword and update it in sql batch passing recordID between
      // commands
      final String sql3 =
          "let var1 = (INSERT INTO Actor2 CONTENT {Name:\"Bingo owner\"} RETURN @this) "
              + "; let var2 = (UPDATE $var1 SET Bingo=1 RETURN AFTER) "
              + "return $var2";
      try (var resSql3ResultSet = db.command(sql3)) {
        var res_sql3 = resSql3ResultSet.next().<Identifiable>getProperty("$var2");
        final EntityImpl sql3doc = res_sql3.getRecord(db);
        Assert.assertEquals(sql3doc.<Object>field("Bingo"), 1);
        Assert.assertEquals(sql3doc.field("Name"), "Bingo owner");
      }
    }
  }

  @Test
  public void testAutoConversionOfEmbeddededSetNoLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db, "embeddedSetNoLinkedClass", PropertyType.EMBEDDEDSET);

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedSetNoLinkedClass',"
                    + " embeddedSetNoLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedSetNoLinkedClass") instanceof Set);

    Set addr = doc.getProperty("embeddedSetNoLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof Map);
    }
  }

  @Test
  public void testAutoConversionOfEmbeddededSetWithLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db,
        "embeddedSetWithLinkedClass",
        PropertyType.EMBEDDEDSET,
        db.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedSetWithLinkedClass',"
                    + " embeddedSetWithLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedSetWithLinkedClass") instanceof Set);

    Set addr = doc.getProperty("embeddedSetWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof EntityImpl);
      Assert.assertEquals(((EntityImpl) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  @Test
  public void testAutoConversionOfEmbeddededListNoLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db, "embeddedListNoLinkedClass", PropertyType.EMBEDDEDLIST);

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedListNoLinkedClass',"
                    + " embeddedListNoLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedListNoLinkedClass") instanceof List);

    List addr = doc.getProperty("embeddedListNoLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof Map);
    }
  }

  @Test
  public void testAutoConversionOfEmbeddededListWithLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    if (!c.existsProperty("embeddedListWithLinkedClass")) {
      c.createProperty(db,
          "embeddedListWithLinkedClass",
          PropertyType.EMBEDDEDLIST,
          db.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));
    }

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedListWithLinkedClass',"
                    + " embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);

    List addr = doc.getProperty("embeddedListWithLinkedClass");
    for (Object o : addr) {
      db.begin();
      Assert.assertTrue(o instanceof EntityImpl);
      Assert.assertEquals(((EntityImpl) o).getClassName(), "TestConvertLinkedClass");
      db.commit();
    }
  }

  @Test
  public void testAutoConversionOfEmbeddededMapNoLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db, "embeddedMapNoLinkedClass", PropertyType.EMBEDDEDMAP);

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedMapNoLinkedClass',"
                    + " embeddedMapNoLinkedClass = {test:{'line1':'123 Fake Street'}}")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedMapNoLinkedClass") instanceof Map);

    Map addr = doc.getProperty("embeddedMapNoLinkedClass");
    for (Object o : addr.values()) {
      Assert.assertTrue(o instanceof Map);
    }
  }

  @Test(enabled = false)
  public void testAutoConversionOfEmbeddededMapWithLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db,
        "embeddedMapWithLinkedClass",
        PropertyType.EMBEDDEDMAP,
        db.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    db.begin();
    var doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedMapWithLinkedClass',"
                    + " embeddedMapWithLinkedClass = {test:{'line1':'123 Fake Street'}}")
            .next()
            .getEntity()
            .orElseThrow();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedMapWithLinkedClass") instanceof Map);

    Map addr = doc.getProperty("embeddedMapWithLinkedClass");
    for (Object o : addr.values()) {
      Assert.assertTrue(o instanceof EntityImpl);
      Assert.assertEquals(((EntityImpl) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  @Test(enabled = false)
  public void testAutoConversionOfEmbeddededNoLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db, "embeddedNoLinkedClass", PropertyType.EMBEDDED);

    db.begin();
    var doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedNoLinkedClass',"
                    + " embeddedNoLinkedClass = {'line1':'123 Fake Street'}")
            .next()
            .getEntity()
            .orElseThrow();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedNoLinkedClass") instanceof EntityImpl);
  }

  @Test
  public void testEmbeddedDates() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestEmbeddedDates");

    db.begin();
    db
        .command(
            "insert into TestEmbeddedDates set events = [{\"on\": date(\"2005-09-08 04:00:00\","
                + " \"yyyy-MM-dd HH:mm:ss\", \"UTC\")}]\n")
        .close();
    db.commit();

    List<Result> result =
        db.query("select from TestEmbeddedDates").stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
    boolean found = false;
    Result doc = result.get(0);
    Collection events = doc.getProperty("events");
    for (Object event : events) {
      Assert.assertTrue(event instanceof Map);
      Object dateObj = ((Map) event).get("on");
      Assert.assertTrue(dateObj instanceof Date);
      Calendar cal = new GregorianCalendar();
      cal.setTime((Date) dateObj);
      Assert.assertEquals(cal.get(Calendar.YEAR), 2005);
      found = true;
    }

    db.begin();
    db.delete(doc.getIdentity().get());
    db.commit();

    Assert.assertTrue(found);
  }

  @Test
  public void testAutoConversionOfEmbeddededWithLinkedClass() {
    SchemaClass c = db.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty(db,
        "embeddedWithLinkedClass",
        PropertyType.EMBEDDED,
        db.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedWithLinkedClass',"
                    + " embeddedWithLinkedClass = {'line1':'123 Fake Street'}")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("embeddedWithLinkedClass") instanceof EntityImpl);
    Assert.assertEquals(
        ((EntityImpl) doc.getProperty("embeddedWithLinkedClass")).getClassName(),
        "TestConvertLinkedClass");
  }

  @Test
  public void testInsertEmbeddedWithRecordAttributes() {
    SchemaClass c = db.getMetadata().getSchema()
        .getOrCreateClass("EmbeddedWithRecordAttributes");
    c.createProperty(db,
        "like",
        PropertyType.EMBEDDED,
        db.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes_Like"));

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO EmbeddedWithRecordAttributes SET `like` = { \n"
                    + "      count: 0, \n"
                    + "      latest: [], \n"
                    + "      '@type': 'document', \n"
                    + "      '@class': 'EmbeddedWithRecordAttributes_Like'\n"
                    + "    } ")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("like") instanceof Identifiable);
    Assert.assertEquals(
        ((EntityImpl) doc.getProperty("like")).getClassName(),
        "EmbeddedWithRecordAttributes_Like");
    Assert.assertEquals(((Entity) doc.getProperty("like")).<Object>getProperty("count"), 0);
  }

  @Test
  public void testInsertEmbeddedWithRecordAttributes2() {
    SchemaClass c = db.getMetadata().getSchema()
        .getOrCreateClass("EmbeddedWithRecordAttributes2");
    c.createProperty(db,
        "like",
        PropertyType.EMBEDDED,
        db.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes2_Like"));

    db.begin();
    Entity doc =
        db
            .command(
                "INSERT INTO EmbeddedWithRecordAttributes2 SET `like` = { \n"
                    + "      count: 0, \n"
                    + "      latest: [], \n"
                    + "      @type: 'document', \n"
                    + "      @class: 'EmbeddedWithRecordAttributes2_Like'\n"
                    + "    } ")
            .next()
            .getEntity()
            .get();
    db.commit();

    doc = db.bindToSession(doc);
    Assert.assertTrue(doc.getProperty("like") instanceof Identifiable);
    Assert.assertEquals(
        ((EntityImpl) doc.getProperty("like")).getClassName(),
        "EmbeddedWithRecordAttributes2_Like");
    Assert.assertEquals(((Entity) doc.getProperty("like")).<Object>getProperty("count"), 0);
  }

  @Test
  public void testInsertWithClusterAsFieldName() {
    SchemaClass c = db.getMetadata().getSchema()
        .getOrCreateClass("InsertWithClusterAsFieldName");

    db.begin();
    db
        .command("INSERT INTO InsertWithClusterAsFieldName ( `cluster` ) values ( 'foo' )")
        .close();
    db.commit();

    List<Result> result =
        db.query("SELECT FROM InsertWithClusterAsFieldName").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getProperty("cluster"), "foo");
  }

  @Test
  public void testInsertEmbeddedBigDecimal() {
    // issue #6670
    db.getMetadata().getSchema().getOrCreateClass("TestInsertEmbeddedBigDecimal");
    db
        .command("create property TestInsertEmbeddedBigDecimal.ed embeddedlist decimal")
        .close();

    db.begin();
    db
        .command("INSERT INTO TestInsertEmbeddedBigDecimal CONTENT {\"ed\": [5,null,5]}")
        .close();
    db.commit();

    List<Result> result =
        db.query("SELECT FROM TestInsertEmbeddedBigDecimal").stream()
            .collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
    Iterable ed = result.get(0).getProperty("ed");
    Object o = ed.iterator().next();
    Assert.assertEquals(o.getClass(), BigDecimal.class);
    Assert.assertEquals(((BigDecimal) o).intValue(), 5);
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final RecordIteratorCluster<?> iteratorCluster =
        db.browseCluster(db.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext()) {
        break;
      }
      Record doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }
}
