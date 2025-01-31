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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CRUDTest extends BaseDBTest {

  protected long startRecordNumber;

  private Entity rome;

  @Parameters(value = "remote")
  public CRUDTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }


  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    createSimpleTestClass();
    createSimpleArrayTestClass();
    createBinaryTestClass();
    createComplexTestClass();
    createPersonClass();
    createEventClass();
    createAgendaClass();
    createNonGenericClass();
    createMediaClass();
    createParentChildClasses();
  }

  @Test
  public void create() {
    startRecordNumber = db.countClass("Account");

    Entity address;

    db.begin();
    var country = db.newEntity("Country");
    country.setProperty("name", "Italy");
    country.save();

    rome = db.newEntity("City");
    rome.setProperty("name", "Rome");
    rome.setProperty("country", country);
    db.save(rome);

    address = db.newEntity("Address");
    address.setProperty("type", "Residence");
    address.setProperty("street", "Piazza Navona, 1");
    address.setProperty("city", rome);
    db.save(address);

    for (var i = startRecordNumber; i < startRecordNumber + TOT_RECORDS_ACCOUNT; ++i) {
      var account = db.newEntity("Account");
      account.setProperty("id", i);
      account.setProperty("name", "Bill");
      account.setProperty("surname", "Gates");
      account.setProperty("birthDate", new Date());
      account.setProperty("salary", (i + 300.10f));
      account.setProperty("addresses", Collections.singletonList(address));
      db.save(account);
    }
    db.commit();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    Assert.assertEquals(db.countClass("Account") - startRecordNumber, TOT_RECORDS_ACCOUNT);
  }

  @Test(dependsOnMethods = "testCreate")
  public void testCreateClass() {
    var schema = db.getMetadata().getSchema();
    Assert.assertNull(schema.getClass("Dummy"));
    var dummyClass = schema.createClass("Dummy");
    dummyClass.createProperty(db, "name", PropertyType.STRING);

    Assert.assertEquals(db.countClass("Dummy"), 0);
    Assert.assertNotNull(schema.getClass("Dummy"));
  }

  @Test
  public void testSimpleTypes() {
    var element = db.newEntity("JavaSimpleTestClass");
    Assert.assertEquals(element.getProperty("text"), "initTest");

    db.begin();
    var date = new Date();
    element.setProperty("text", "test");
    element.setProperty("numberSimple", 12345);
    element.setProperty("doubleSimple", 12.34d);
    element.setProperty("floatSimple", 123.45f);
    element.setProperty("longSimple", 12345678L);
    element.setProperty("byteSimple", (byte) 1);
    element.setProperty("flagSimple", true);
    element.setProperty("dateField", date);

    db.save(element);
    db.commit();

    var id = element.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    EntityImpl loadedRecord = db.load(id);
    Assert.assertEquals(loadedRecord.getProperty("text"), "test");
    Assert.assertEquals(loadedRecord.<Integer>getProperty("numberSimple"), 12345);
    Assert.assertEquals(loadedRecord.<Double>getProperty("doubleSimple"), 12.34d);
    Assert.assertEquals(loadedRecord.<Float>getProperty("floatSimple"), 123.45f);
    Assert.assertEquals(loadedRecord.<Long>getProperty("longSimple"), 12345678L);
    Assert.assertEquals(loadedRecord.<Byte>getProperty("byteSimple"), (byte) 1);
    Assert.assertEquals(loadedRecord.<Boolean>getProperty("flagSimple"), true);
    Assert.assertEquals(loadedRecord.getProperty("dateField"), date);
    db.commit();
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testSimpleArrayTypes() {
    var element = db.newInstance("JavaSimpleArraysTestClass");
    var textArray = new String[10];
    var intArray = new int[10];
    var longArray = new long[10];
    var doubleArray = new double[10];
    var floatArray = new float[10];
    var booleanArray = new boolean[10];
    var dateArray = new Date[10];
    var cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.YEAR, 1900);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    for (var i = 0; i < 10; i++) {
      textArray[i] = i + "";
      intArray[i] = i;
      longArray[i] = i;
      doubleArray[i] = i;
      floatArray[i] = i;
      booleanArray[i] = (i % 2 == 0);
      cal.set(Calendar.DAY_OF_MONTH, (i + 1));
      dateArray[i] = cal.getTime();
    }

    element.setProperty("text", textArray);
    element.setProperty("dateField", dateArray);
    element.setProperty("doubleSimple", doubleArray);
    element.setProperty("flagSimple", booleanArray);
    element.setProperty("floatSimple", floatArray);
    element.setProperty("longSimple", longArray);
    element.setProperty("numberSimple", intArray);

    Assert.assertNotNull(element.getProperty("text"));
    Assert.assertNotNull(element.getProperty("numberSimple"));
    Assert.assertNotNull(element.getProperty("longSimple"));
    Assert.assertNotNull(element.getProperty("doubleSimple"));
    Assert.assertNotNull(element.getProperty("floatSimple"));
    Assert.assertNotNull(element.getProperty("flagSimple"));
    Assert.assertNotNull(element.getProperty("dateField"));

    db.begin();
    db.save(element);
    db.commit();
    var id = element.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loadedElement = db.load(id);
    Assert.assertNotNull(loadedElement.getProperty("text"));
    Assert.assertNotNull(loadedElement.getProperty("numberSimple"));
    Assert.assertNotNull(loadedElement.getProperty("longSimple"));
    Assert.assertNotNull(loadedElement.getProperty("doubleSimple"));
    Assert.assertNotNull(loadedElement.getProperty("floatSimple"));
    Assert.assertNotNull(loadedElement.getProperty("flagSimple"));
    Assert.assertNotNull(loadedElement.getProperty("dateField"));

    Assert.assertEquals(loadedElement.<List<String>>getProperty("text").size(), 10);
    Assert.assertEquals(loadedElement.<List<Integer>>getProperty("numberSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Long>>getProperty("longSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Double>>getProperty("doubleSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Float>>getProperty("floatSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Boolean>>getProperty("flagSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Date>>getProperty("dateField").size(), 10);

    for (var i = 0; i < 10; i++) {
      Assert.assertEquals(loadedElement.<List<String>>getProperty("text").get(i), i + "");
      Assert.assertEquals(loadedElement.<List<Integer>>getProperty("numberSimple").get(i), i);
      Assert.assertEquals(loadedElement.<List<Long>>getProperty("longSimple").get(i), i);
      Assert.assertEquals(loadedElement.<List<Double>>getProperty("doubleSimple").get(i), i);
      Assert.assertEquals(loadedElement.<List<Float>>getProperty("floatSimple").get(i), (float) i);
      Assert.assertEquals(
          loadedElement.<List<Boolean>>getProperty("flagSimple").get(i), (i % 2 == 0));
      cal.set(Calendar.DAY_OF_MONTH, (i + 1));
      Assert.assertEquals(loadedElement.<List<Date>>getProperty("dateField").get(i), cal.getTime());
    }

    for (var i = 0; i < 10; i++) {
      var j = i + 10;
      textArray[i] = j + "";
      intArray[i] = j;
      longArray[i] = j;
      doubleArray[i] = j;
      floatArray[i] = j;
      booleanArray[i] = (j % 2 == 0);
      cal.set(Calendar.DAY_OF_MONTH, (j + 1));
      dateArray[i] = cal.getTime();
    }
    loadedElement.setProperty("text", textArray);
    loadedElement.setProperty("dateField", dateArray);
    loadedElement.setProperty("doubleSimple", doubleArray);
    loadedElement.setProperty("flagSimple", booleanArray);
    loadedElement.setProperty("floatSimple", floatArray);
    loadedElement.setProperty("longSimple", longArray);
    loadedElement.setProperty("numberSimple", intArray);

    db.save(loadedElement);
    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    loadedElement = db.load(id);
    Assert.assertNotNull(loadedElement.getProperty("text"));
    Assert.assertNotNull(loadedElement.getProperty("numberSimple"));
    Assert.assertNotNull(loadedElement.getProperty("longSimple"));
    Assert.assertNotNull(loadedElement.getProperty("doubleSimple"));
    Assert.assertNotNull(loadedElement.getProperty("floatSimple"));
    Assert.assertNotNull(loadedElement.getProperty("flagSimple"));
    Assert.assertNotNull(loadedElement.getProperty("dateField"));

    Assert.assertEquals(loadedElement.<List<String>>getProperty("text").size(), 10);
    Assert.assertEquals(loadedElement.<List<Integer>>getProperty("numberSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Long>>getProperty("longSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Double>>getProperty("doubleSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Float>>getProperty("floatSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Boolean>>getProperty("flagSimple").size(), 10);
    Assert.assertEquals(loadedElement.<List<Date>>getProperty("dateField").size(), 10);

    for (var i = 0; i < 10; i++) {
      var j = i + 10;
      Assert.assertEquals(loadedElement.<List<String>>getProperty("text").get(i), j + "");
      Assert.assertEquals(loadedElement.<List<Integer>>getProperty("numberSimple").get(i), j);
      Assert.assertEquals(loadedElement.<List<Long>>getProperty("longSimple").get(i), j);
      Assert.assertEquals(loadedElement.<List<Double>>getProperty("doubleSimple").get(i), j);
      Assert.assertEquals(loadedElement.<List<Float>>getProperty("floatSimple").get(i), (float) j);
      Assert.assertEquals(
          loadedElement.<List<Boolean>>getProperty("flagSimple").get(i), (j % 2 == 0));

      cal.set(Calendar.DAY_OF_MONTH, (j + 1));
      Assert.assertEquals(loadedElement.<List<Date>>getProperty("dateField").get(i), cal.getTime());
    }

    db.commit();
    db.close();

    db = createSessionInstance();

    db.begin();
    loadedElement = db.load(id);

    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("text")).iterator().next() instanceof String);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("numberSimple")).iterator().next()
            instanceof Integer);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("longSimple")).iterator().next()
            instanceof Long);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("doubleSimple")).iterator().next()
            instanceof Double);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("floatSimple")).iterator().next()
            instanceof Float);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("flagSimple")).iterator().next()
            instanceof Boolean);
    Assert.assertTrue(
        ((Collection<?>) loadedElement.getProperty("dateField")).iterator().next() instanceof Date);

    db.delete(id);
    db.commit();
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testBinaryDataType() {
    var element = db.newInstance("JavaBinaryDataTestClass");
    var bytes = new byte[10];
    for (var i = 0; i < 10; i++) {
      bytes[i] = (byte) i;
    }

    element.setProperty("binaryData", bytes);

    var fieldName = "binaryData";
    Assert.assertNotNull(element.getProperty(fieldName));

    db.begin();
    db.save(element);
    db.commit();

    var id = element.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loadedElement = db.load(id);
    Assert.assertNotNull(loadedElement.getProperty(fieldName));

    Assert.assertEquals(loadedElement.<byte[]>getProperty("binaryData").length, 10);
    Assert.assertEquals(loadedElement.getProperty("binaryData"), bytes);

    for (var i = 0; i < 10; i++) {
      var j = i + 10;
      bytes[i] = (byte) j;
    }
    loadedElement.setProperty("binaryData", bytes);

    db.save(loadedElement);
    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    loadedElement = db.load(id);
    Assert.assertNotNull(loadedElement.getProperty(fieldName));

    Assert.assertEquals(loadedElement.<byte[]>getProperty("binaryData").length, 10);
    Assert.assertEquals(loadedElement.getProperty("binaryData"), bytes);

    db.commit();
    db.close();

    db = createSessionInstance();

    db.begin();
    db.delete(id);
    db.commit();
  }

  @Test(dependsOnMethods = "testSimpleArrayTypes")
  public void collectionsDocumentTypeTestPhaseOne() {
    db.begin();
    var a = db.newInstance("JavaComplexTestClass");

    for (var i = 0; i < 3; i++) {
      var child1 = db.newEntity("Child");
      var child2 = db.newEntity("Child");
      var child3 = db.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }

    a = db.save(a);
    db.commit();

    var rid = a.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    var agendas = executeQuery("SELECT FROM " + rid);
    var testLoadedEntity = (EntityImpl) agendas.getFirst().asEntity();

    checkCollectionImplementations(testLoadedEntity);

    db.save(testLoadedEntity);
    db.commit();

    db.freeze(false);
    db.release();

    db.begin();

    testLoadedEntity = db.load(rid);

    checkCollectionImplementations(testLoadedEntity);
    db.commit();
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseOne")
  public void collectionsDocumentTypeTestPhaseTwo() {
    db.begin();
    var a = db.newInstance("JavaComplexTestClass");

    for (var i = 0; i < 10; i++) {
      var child1 = db.newEntity("Child");
      var child2 = db.newEntity("Child");
      var child3 = db.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }

    a = db.save(a);
    db.commit();

    var rid = a.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    var agendas = executeQuery("SELECT FROM " + rid);
    var testLoadedEntity = (EntityImpl) agendas.getFirst().asEntity();

    checkCollectionImplementations(testLoadedEntity);

    testLoadedEntity = db.save(testLoadedEntity);
    db.commit();

    db.freeze(false);
    db.release();

    db.begin();
    checkCollectionImplementations(db.bindToSession(testLoadedEntity));
    db.commit();
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseTwo")
  public void collectionsDocumentTypeTestPhaseThree() {
    var a = db.newInstance("JavaComplexTestClass");

    db.begin();
    for (var i = 0; i < 100; i++) {
      var child1 = db.newEntity("Child");
      var child2 = db.newEntity("Child");
      var child3 = db.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }
    a = db.save(a);
    db.commit();

    var rid = a.getIdentity();
    db.close();

    db = createSessionInstance();
    db.begin();
    var agendas = executeQuery("SELECT FROM " + rid);
    var testLoadedEntity = (EntityImpl) agendas.getFirst();
    checkCollectionImplementations(testLoadedEntity);

    testLoadedEntity = db.save(testLoadedEntity);
    db.commit();

    db.freeze(false);
    db.release();

    db.begin();
    checkCollectionImplementations(db.bindToSession(testLoadedEntity));
    db.rollback();
  }

  protected static void checkCollectionImplementations(EntityImpl doc) {
    var collectionObj = doc.field("list");
    var validImplementation =
        (collectionObj instanceof TrackedList<?>) || (doc.field("list") instanceof LinkList);
    if (!validImplementation) {
      Assert.fail(
          "Document list implementation "
              + collectionObj.getClass().getName()
              + " not compatible with current Object Database loading management");
    }
    collectionObj = doc.field("set");
    validImplementation =
        collectionObj instanceof TrackedSet<?>;
    if (!validImplementation) {
      Assert.fail(
          "Document set implementation "
              + collectionObj.getClass().getName()
              + " not compatible with current Object Database management");
    }
    collectionObj = doc.field("children");
    validImplementation = collectionObj instanceof TrackedMap<?>;
    if (!validImplementation) {
      Assert.fail(
          "Document map implementation "
              + collectionObj.getClass().getName()
              + " not compatible with current Object Database management");
    }
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testDateInTransaction() {
    var element = db.newEntity("JavaSimpleTestClass");
    var date = new Date();
    element.setProperty("dateField", date);
    db.begin();
    element.save();
    db.commit();

    db.begin();
    element = db.bindToSession(element);
    Assert.assertEquals(element.<List<Date>>getProperty("dateField"), date);
    db.commit();
  }

  @Test(dependsOnMethods = "testCreateClass")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    db.begin();
    rome = db.bindToSession(rome);
    Set<Integer> ids = new HashSet<>(TOT_RECORDS_ACCOUNT);
    for (var i = 0; i < TOT_RECORDS_ACCOUNT; i++) {
      ids.add(i);
    }

    for (Entity a : db.browseClass("Account")) {
      int id = a.<Integer>getProperty("id");
      Assert.assertTrue(ids.remove(id));

      Assert.assertEquals(a.<Integer>getProperty("id"), id);
      Assert.assertEquals(a.getProperty("name"), "Bill");
      Assert.assertEquals(a.getProperty("surname"), "Gates");
      Assert.assertEquals(a.<Float>getProperty("salary"), id + 300.1f);
      Assert.assertEquals(a.<List<Identifiable>>getProperty("addresses").size(), 1);
      Assert.assertEquals(
          a.<List<Identifiable>>getProperty("addresses")
              .getFirst()
              .<Entity>getRecord(db)
              .getEntityProperty("city")
              .getProperty("name"),
          rome.<String>getProperty("name"));
      Assert.assertEquals(
          a.<List<Identifiable>>getProperty("addresses")
              .getFirst()
              .<Entity>getRecord(db)
              .getEntityProperty("city")
              .getEntityProperty("country")
              .getProperty("name"),
          rome.<Entity>getRecord(db)
              .<Identifiable>getProperty("country")
              .<Entity>getRecord(db)
              .<String>getProperty("name"));
    }

    Assert.assertTrue(ids.isEmpty());
    db.commit();
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void mapEnumAndInternalObjects() {
    db.executeInTxBatches((Iterator<EntityImpl>) db.browseClass("OUser"),
        ((session, document) -> {
          document.save();
        }));

  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void mapObjectsLinkTest() {
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    var c = db.newInstance("Child");
    c.setProperty("name", "John");

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Jack");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Bob");

    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Sam");

    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Dean");

    var list = new ArrayList<Identifiable>();
    list.add(c1);
    list.add(c2);
    list.add(c3);
    list.add(c4);

    p.setProperty("list", list);

    var children = new HashMap<String, Entity>();
    children.put("first", c);
    p.setProperty("children", children);

    db.begin();
    db.save(p);
    db.commit();

    db.begin();
    var cresult = executeQuery("select * from Child");
    Assert.assertFalse(cresult.isEmpty());

    var rid = p.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    var loaded = db.<Entity>load(rid);

    list = loaded.getProperty("list");
    Assert.assertEquals(list.size(), 4);
    Assert.assertEquals(
        Objects.requireNonNull(list.get(0).<Entity>getRecord(db).getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(1).<Entity>getRecord(db).getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(2).<Entity>getRecord(db).getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(3).<Entity>getRecord(db).getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(list.get(0).<Entity>getRecord(db).getProperty("name"), "Jack");
    Assert.assertEquals(list.get(1).<Entity>getRecord(db).getProperty("name"), "Bob");
    Assert.assertEquals(list.get(2).<Entity>getRecord(db).getProperty("name"), "Sam");
    Assert.assertEquals(list.get(3).<Entity>getRecord(db).getProperty("name"), "Dean");
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void listObjectsLinkTest() {
    db.begin();
    var hanSolo = db.newInstance("PersonTest");
    hanSolo.setProperty("firstName", "Han");
    hanSolo = db.save(hanSolo);
    db.commit();

    db.begin();
    var obiWan = db.newInstance("PersonTest");
    obiWan.setProperty("firstName", "Obi-Wan");
    obiWan = db.save(obiWan);

    var luke = db.newInstance("PersonTest");
    luke.setProperty("firstName", "Luke");
    luke = db.save(luke);
    db.commit();

    // ============================== step 1
    // add new information to luke
    db.begin();
    luke = db.bindToSession(luke);
    var friends = new HashSet<Identifiable>();
    friends.add(db.bindToSession(hanSolo));

    luke.setProperty("friends", friends);
    db.save(luke);
    db.commit();

    db.begin();
    luke = db.bindToSession(luke);
    Assert.assertEquals(luke.<Set<Identifiable>>getProperty("friends").size(), 1);
    friends = new HashSet<>();
    friends.add(db.bindToSession(obiWan));
    luke.setProperty("friends", friends);

    db.save(db.bindToSession(luke));
    db.commit();

    db.begin();
    luke = db.bindToSession(luke);
    Assert.assertEquals(luke.<Set<Identifiable>>getProperty("friends").size(), 1);
    db.commit();
    // ============================== end 2
  }

  @Test(dependsOnMethods = "listObjectsLinkTest")
  public void listObjectsIterationTest() {
    var a = db.newInstance("Agenda");

    for (var i = 0; i < 10; i++) {
      a.setProperty("events", Collections.singletonList(db.newInstance("Event")));
    }
    db.begin();
    a = db.save(a);
    db.commit();
    var rid = a.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    var agendas = executeQuery("SELECT FROM " + rid);
    var agenda = agendas.getFirst().asEntity();
    //noinspection unused,StatementWithEmptyBody
    for (var e : agenda.<List<Entity>>getProperty("events")) {
      // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
    }

    agenda = db.save(agenda);
    db.commit();

    db.freeze(false);
    db.release();

    db.begin();
    agenda = db.bindToSession(agenda);
    try {
      for (var i = 0; i < agenda.<List<Entity>>getProperty("events").size(); i++) {
        @SuppressWarnings("unused")
        var e = agenda.<List<Entity>>getProperty("events").get(i);
        // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
      }
    } catch (ConcurrentModificationException cme) {
      Assert.fail("Error iterating Object list", cme);
    }

    if (db.getTransaction().isActive()) {
      db.rollback();
    }
  }

  @Test(dependsOnMethods = "listObjectsIterationTest")
  public void mapObjectsListEmbeddedTest() {
    db.begin();
    var cresult = executeQuery("select * from Child");

    var childSize = cresult.size();

    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    var c = db.newInstance("Child");
    c.setProperty("name", "John");

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Jack");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Bob");

    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Sam");

    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Dean");

    var list = new ArrayList<Identifiable>();
    list.add(c1);
    list.add(c2);
    list.add(c3);
    list.add(c4);

    p.setProperty("embeddedList", list);

    db.save(p);
    db.commit();

    db.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    var rid = p.getIdentity();
    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    Assert.assertEquals(loaded.<List<Entity>>getProperty("embeddedList").size(), 4);
    Assert.assertTrue(loaded.<List<Entity>>getProperty("embeddedList").get(0).isEmbedded());
    Assert.assertTrue(loaded.<List<Entity>>getProperty("embeddedList").get(1).isEmbedded());
    Assert.assertTrue(loaded.<List<Entity>>getProperty("embeddedList").get(2).isEmbedded());
    Assert.assertTrue(loaded.<List<Entity>>getProperty("embeddedList").get(3).isEmbedded());
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(0)
                    .<Entity>getRecord(db)
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(1)
                    .<Entity>getRecord(db)
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(2)
                    .getEntity(db)
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(3)
                    .getEntity(db)
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        loaded.<List<Entity>>getProperty("embeddedList").get(0).getProperty("name"), "Jack");
    Assert.assertEquals(
        loaded.<List<Entity>>getProperty("embeddedList").get(1).getProperty("name"), "Bob");
    Assert.assertEquals(
        loaded.<List<Entity>>getProperty("embeddedList").get(2).getProperty("name"), "Sam");
    Assert.assertEquals(
        loaded.<List<Entity>>getProperty("embeddedList").get(3).getProperty("name"), "Dean");
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsListEmbeddedTest")
  public void mapObjectsSetEmbeddedTest() {
    db.begin();
    var cresult = executeQuery("select * from Child");
    var childSize = cresult.size();

    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    var c = db.newInstance("Child");
    c.setProperty("name", "John");

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Jack");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Bob");

    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Sam");

    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Dean");

    var embeddedSet = new HashSet<Entity>();
    embeddedSet.add(c);
    embeddedSet.add(c1);
    embeddedSet.add(c2);
    embeddedSet.add(c3);
    embeddedSet.add(c4);

    p.setProperty("embeddedSet", embeddedSet);

    db.save(p);
    db.commit();

    db.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    var rid = p.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    Assert.assertEquals(loaded.<Set<Entity>>getProperty("embeddedSet").size(), 5);
    for (var loadedC : loaded.<Set<Entity>>getProperty("embeddedSet")) {
      Assert.assertTrue(loadedC.isEmbedded());
      Assert.assertEquals(loadedC.getClassName(), "Child");
      Assert.assertTrue(
          loadedC.<String>getProperty("name").equals("John")
              || loadedC.<String>getProperty("name").equals("Jack")
              || loadedC.<String>getProperty("name").equals("Bob")
              || loadedC.<String>getProperty("name").equals("Sam")
              || loadedC.<String>getProperty("name").equals("Dean"));
    }
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsSetEmbeddedTest")
  public void mapObjectsMapEmbeddedTest() {
    db.begin();
    var cresult = executeQuery("select * from Child");

    var childSize = cresult.size();
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    var c = db.newInstance("Child");
    c.setProperty("name", "John");

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Jack");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Bob");

    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Sam");

    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Dean");

    var embeddedChildren = new HashMap<String, Entity>();
    embeddedChildren.put(c.getProperty("name"), c);
    embeddedChildren.put(c1.getProperty("name"), c1);
    embeddedChildren.put(c2.getProperty("name"), c2);
    embeddedChildren.put(c3.getProperty("name"), c3);
    embeddedChildren.put(c4.getProperty("name"), c4);

    p.setProperty("embeddedChildren", embeddedChildren);

    db.save(p);
    db.commit();

    db.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    var rid = p.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    Assert.assertEquals(loaded.<Map<String, Entity>>getProperty("embeddedChildren").size(), 5);
    for (var key : loaded.<Map<String, Entity>>getProperty("embeddedChildren").keySet()) {
      var loadedC = loaded.<Map<String, Entity>>getProperty("embeddedChildren").get(key);
      Assert.assertTrue(loadedC.isEmbedded());
      Assert.assertEquals(loadedC.getClassName(), "Child");
      Assert.assertTrue(
          loadedC.<String>getProperty("name").equals("John")
              || loadedC.<String>getProperty("name").equals("Jack")
              || loadedC.<String>getProperty("name").equals("Bob")
              || loadedC.<String>getProperty("name").equals("Sam")
              || loadedC.<String>getProperty("name").equals("Dean"));
    }
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsNonExistingKeyTest() {
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    db.begin();
    p = db.save(p);

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "John");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Jack");

    var children = new HashMap<String, Entity>();
    children.put("first", c1);
    children.put("second", c2);

    p.setProperty("children", children);

    db.save(p);
    db.commit();

    db.begin();
    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Olivia");
    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Peter");

    p = db.bindToSession(p);
    p.<Map<String, Identifiable>>getProperty("children").put("third", c3);
    p.<Map<String, Identifiable>>getProperty("children").put("fourth", c4);

    db.save(p);
    db.commit();

    db.begin();
    var cresult = executeQuery("select * from Child");
    Assert.assertFalse(cresult.isEmpty());

    var rid = p.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    c1 = db.bindToSession(c1);
    c2 = db.bindToSession(c2);
    c3 = db.bindToSession(c3);
    c4 = db.bindToSession(c4);

    Entity loaded = db.load(rid);

    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("first")
            .getEntity(db)
            .getProperty("name"),
        c1.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("second")
            .getEntity(db)
            .getProperty("name"),
        c2.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("third")
            .getEntity(db)
            .getProperty("name"),
        c3.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("fourth")
            .getEntity(db)
            .getProperty("name"),
        c4.<String>getProperty("name"));
    Assert.assertNull(loaded.<Map<String, Identifiable>>getProperty("children").get("fifth"));
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkTwoSaveTest() {
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    db.begin();
    p = db.save(p);

    var c1 = db.newInstance("Child");
    c1.setProperty("name", "John");

    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Jack");

    var children = new HashMap<String, Identifiable>();
    children.put("first", c1);
    children.put("second", c2);

    p.setProperty("children", children);

    db.save(p);

    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Olivia");
    var c4 = db.newInstance("Child");
    c4.setProperty("name", "Peter");

    p.<Map<String, Identifiable>>getProperty("children").put("third", c3);
    p.<Map<String, Identifiable>>getProperty("children").put("fourth", c4);

    db.save(p);
    db.commit();

    db.begin();
    var cresult = executeQuery("select * from Child");
    Assert.assertFalse(cresult.isEmpty());

    var rid = p.getIdentity();
    db.commit();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    c1 = db.bindToSession(c1);
    c2 = db.bindToSession(c2);
    c3 = db.bindToSession(c3);
    c4 = db.bindToSession(c4);

    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("first")
            .getEntity(db)
            .getProperty("name"),
        c1.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("second")
            .getEntity(db)
            .getProperty("name"),
        c2.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("third")
            .getEntity(db)
            .getProperty("name"),
        c3.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, Identifiable>>getProperty("children")
            .get("fourth")
            .getEntity(db)
            .getProperty("name"),
        c4.<String>getProperty("name"));
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkUpdateDatabaseNewInstanceTest() {
    // TEST WITH NEW INSTANCE
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Fringe");

    var c = db.newInstance("Child");
    c.setProperty("name", "Peter");
    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Walter");
    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Olivia");
    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Astrid");

    Map<String, Identifiable> children = new HashMap<>();
    children.put(c.getProperty("name"), c);
    children.put(c1.getProperty("name"), c1);
    children.put(c2.getProperty("name"), c2);
    children.put(c3.getProperty("name"), c3);

    p.setProperty("children", children);

    db.begin();
    db.save(p);
    db.commit();

    var rid = p.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    for (var key : loaded.<Map<String, Identifiable>>getProperty("children").keySet()) {
      Assert.assertTrue(
          key.equals("Peter")
              || key.equals("Walter")
              || key.equals("Olivia")
              || key.equals("Astrid"));
      Assert.assertEquals(
          loaded
              .<Map<String, Identifiable>>getProperty("children")
              .get(key)
              .getEntity(db)
              .getClassName(),
          "Child");
      Assert.assertEquals(
          key,
          loaded
              .<Map<String, Identifiable>>getProperty("children")
              .get(key)
              .getEntity(db)
              .getProperty("name"));
      switch (key) {
        case "Peter" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Peter");
        case "Walter" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Walter");
        case "Olivia" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Olivia");
        case "Astrid" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Astrid");
      }
    }
    db.commit();

    db.begin();
    for (Entity reloaded : db.browseClass("JavaComplexTestClass")) {
      reloaded = db.bindToSession(reloaded);
      var c4 = db.newInstance("Child");
      c4.setProperty("name", "The Observer");

      children = reloaded.getProperty("children");
      if (children == null) {
        children = new HashMap<>();
        reloaded.setProperty("children", children);
      }

      children.put(c4.getProperty("name"), c4);

      db.save(reloaded);
    }
    db.commit();

    db.close();
    db = createSessionInstance();
    db.begin();
    for (Entity reloaded : db.browseClass("JavaComplexTestClass")) {
      Assert.assertTrue(
          reloaded.<Map<String, Identifiable>>getProperty("children")
              .containsKey("The Observer"));
      Assert.assertNotNull(
          reloaded.<Map<String, Identifiable>>getProperty("children").get("The Observer"));
      Assert.assertEquals(
          reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getEntity(db)
              .getProperty("name"),
          "The Observer");
      Assert.assertTrue(
          reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isPersistent()
              && ((RecordId) reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity())
              .isValid());
    }
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateDatabaseNewInstanceTest")
  public void mapObjectsLinkUpdateJavaNewInstanceTest() {
    // TEST WITH NEW INSTANCE
    db.begin();
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Fringe");

    var c = db.newInstance("Child");
    c.setProperty("name", "Peter");
    var c1 = db.newInstance("Child");
    c1.setProperty("name", "Walter");
    var c2 = db.newInstance("Child");
    c2.setProperty("name", "Olivia");
    var c3 = db.newInstance("Child");
    c3.setProperty("name", "Astrid");

    var children = new HashMap<String, Identifiable>();
    children.put(c.getProperty("name"), c);
    children.put(c1.getProperty("name"), c1);
    children.put(c2.getProperty("name"), c2);
    children.put(c3.getProperty("name"), c3);

    p.setProperty("children", children);

    p = db.save(p);
    db.commit();

    var rid = p.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    for (var key : loaded.<Map<String, Identifiable>>getProperty("children").keySet()) {
      Assert.assertTrue(
          key.equals("Peter")
              || key.equals("Walter")
              || key.equals("Olivia")
              || key.equals("Astrid"));
      Assert.assertEquals(
          loaded
              .<Map<String, Identifiable>>getProperty("children")
              .get(key)
              .getEntity(db)
              .getClassName(),
          "Child");
      Assert.assertEquals(
          key,
          loaded
              .<Map<String, Identifiable>>getProperty("children")
              .get(key)
              .getEntity(db)
              .getProperty("name"));
      switch (key) {
        case "Peter" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Peter");
        case "Walter" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Walter");
        case "Olivia" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Olivia");
        case "Astrid" -> Assert.assertEquals(
            loaded
                .<Map<String, Identifiable>>getProperty("children")
                .get(key)
                .getEntity(db)
                .getProperty("name"),
            "Astrid");
      }
    }

    for (Entity reloaded : db.browseClass("JavaComplexTestClass")) {
      var c4 = db.newInstance("Child");
      c4.setProperty("name", "The Observer");

      reloaded.<Map<String, Identifiable>>getProperty("children").put(c4.getProperty("name"), c4);

      db.save(reloaded);
    }
    db.commit();

    db.close();
    db = createSessionInstance();
    db.begin();
    for (Entity reloaded : db.browseClass("JavaComplexTestClass")) {
      Assert.assertTrue(
          reloaded.<Map<String, Identifiable>>getProperty("children")
              .containsKey("The Observer"));
      Assert.assertNotNull(
          reloaded.<Map<String, Identifiable>>getProperty("children").get("The Observer"));
      Assert.assertEquals(
          reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getEntity(db)
              .getProperty("name"),
          "The Observer");
      Assert.assertTrue(
          reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isPersistent()
              && ((RecordId) reloaded
              .<Map<String, Identifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity())
              .isValid());
    }
    db.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateJavaNewInstanceTest")
  public void mapStringTest() {
    Map<String, String> relatives = new HashMap<>();
    relatives.put("father", "Mike");
    relatives.put("mother", "Julia");

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    var stringMap = new HashMap<String, String>();
    stringMap.put("father", "Mike");
    stringMap.put("mother", "Julia");

    p.setProperty("stringMap", stringMap);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    db.begin();
    db.save(p);
    db.commit();

    var rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    db.save(loaded);
    db.commit();

    db.begin();
    loaded = db.bindToSession(loaded);
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    db.commit();
    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    db.begin();
    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringMap", relatives);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    db.save(loaded);
    db.commit();

    db.begin();
    for (var entry : relatives.entrySet()) {
      loaded = db.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    db.begin();
    // TEST WITH JAVA CONSTRUCTOR
    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringMap", relatives);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    p = db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    db.save(loaded);
    db.commit();

    db.begin();
    for (var entry : relatives.entrySet()) {
      loaded = db.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    db.delete(db.bindToSession(loaded));
    db.commit();
  }

  @Test(dependsOnMethods = "mapStringTest")
  public void setStringTest() {
    db.begin();
    var testClass = db.newInstance("JavaComplexTestClass");
    Set<String> roles = new HashSet<>();

    roles.add("manager");
    roles.add("developer");
    testClass.setProperty("stringSet", roles);

    Entity testClassProxy = db.save(testClass);
    db.commit();

    db.begin();
    testClassProxy = db.bindToSession(testClassProxy);
    Assert.assertEquals(roles.size(), testClassProxy.<Set<String>>getProperty("stringSet").size());
    for (var referenceRole : roles) {
      testClassProxy = db.bindToSession(testClassProxy);
      Assert.assertTrue(
          testClassProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    var orid = testClassProxy.getIdentity();
    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    Entity loadedProxy = db.load(orid);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (var referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    db.save(db.bindToSession(loadedProxy));
    db.commit();

    db.begin();
    loadedProxy = db.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (var referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    loadedProxy.<Set<String>>getProperty("stringSet").remove("developer");
    roles.remove("developer");
    db.save(loadedProxy);
    db.commit();

    db.begin();
    loadedProxy = db.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (var referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }
    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    loadedProxy = db.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (var referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }
    db.commit();
  }

  @Test(dependsOnMethods = "setStringTest")
  public void mapStringListTest() {
    Map<String, List<String>> songAndMovies = new HashMap<>();
    List<String> movies = new ArrayList<>();
    List<String> songs = new ArrayList<>();

    movies.add("Star Wars");
    movies.add("Star Wars: The Empire Strikes Back");
    movies.add("Star Wars: The return of the Jedi");
    songs.add("Metallica - Master of Puppets");
    songs.add("Daft Punk - Harder, Better, Faster, Stronger");
    songs.add("Johnny Cash - Cocaine Blues");
    songs.add("Skrillex - Scary Monsters & Nice Sprites");
    songAndMovies.put("movies", movies);
    songAndMovies.put("songs", songs);

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    p.setProperty("stringListMap", songAndMovies);

    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.begin();
    db.save(p);
    db.commit();

    var rid = p.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    Entity loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = db.newInstance("JavaComplexTestClass");
    db.begin();
    p.setProperty("name", "Chuck");
    p.setProperty("stringListMap", songAndMovies);

    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    db.begin();
    // TEST WITH OBJECT DATABASE NEW INSTANCE LIST DIRECT ADD
    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    var stringListMap = new HashMap<String, List<String>>();
    stringListMap.put("movies", new ArrayList<>());
    stringListMap.get("movies").add("Star Wars");
    stringListMap.get("movies").add("Star Wars: The Empire Strikes Back");
    stringListMap.get("movies").add("Star Wars: The return of the Jedi");

    stringListMap.put("songs", new ArrayList<>());
    stringListMap.get("songs").add("Metallica - Master of Puppets");
    stringListMap.get("songs").add("Daft Punk - Harder, Better, Faster, Stronger");
    stringListMap.get("songs").add("Johnny Cash - Cocaine Blues");
    stringListMap.get("songs").add("Skrillex - Scary Monsters & Nice Sprites");

    p.setProperty("stringListMap", stringListMap);

    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    // TEST WITH JAVA CONSTRUCTOR
    db.begin();
    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringListMap", songAndMovies);

    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    p = db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (var entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();
  }

  @Test
  public void embeddedMapObjectTest() {
    var cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    Map<String, Object> relatives = new HashMap<>();
    relatives.put("father", "Mike");
    relatives.put("mother", "Julia");
    relatives.put("number", 10);
    relatives.put("date", cal.getTime());

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    var mapObject = new HashMap<String, Object>();
    mapObject.put("father", "Mike");
    mapObject.put("mother", "Julia");
    mapObject.put("number", 10);
    mapObject.put("date", cal.getTime());

    p.setProperty("mapObject", mapObject);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    db.begin();
    db.save(p);
    db.commit();

    var rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");
    relatives.put("brother", "Nike");

    db.save(loaded);
    db.commit();

    db.begin();
    for (var entry : relatives.entrySet()) {
      loaded = db.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    db.commit();

    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    db.begin();
    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("mapObject", relatives);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");
    relatives.put("brother", "Nike");

    db.save(loaded);
    db.commit();

    db.begin();
    for (var entry : relatives.entrySet()) {
      loaded = db.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    db.commit();

    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();
    db.begin();

    p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("mapObject", relatives);

    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    p = db.save(p);
    db.commit();

    rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");

    relatives.put("brother", "Nike");
    db.save(loaded);
    db.commit();

    db.begin();
    for (var entry : relatives.entrySet()) {
      loaded = db.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    db.commit();
    db.close();
    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);

    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (var entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    db.delete(db.bindToSession(loaded));
    db.commit();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test(dependsOnMethods = "embeddedMapObjectTest")
  public void testNoGenericCollections() {
    var p = db.newInstance("JavaNoGenericCollectionsTestClass");
    var c1 = db.newInstance("Child");
    c1.setProperty("name", "1");
    var c2 = db.newInstance("Child");
    c2.setProperty("name", "2");
    var c3 = db.newInstance("Child");
    c3.setProperty("name", "3");
    var c4 = db.newInstance("Child");
    c4.setProperty("name", "4");

    var list = new ArrayList();
    var set = new HashSet();
    var map = new HashMap();

    list.add(c1);
    list.add(c2);
    list.add(c3);
    list.add(c4);

    set.add(c1);
    set.add(c2);
    set.add(c3);
    set.add(c4);

    map.put("1", c1);
    map.put("2", c2);
    map.put("3", c3);
    map.put("4", c4);

    p.setProperty("list", list);
    p.setProperty("set", set);
    p.setProperty("map", map);

    db.begin();
    p = db.save(p);
    db.commit();

    var rid = p.getIdentity();
    db.close();
    db = createSessionInstance();
    db.begin();
    p = db.load(rid);

    Assert.assertEquals(p.<List>getProperty("list").size(), 4);
    Assert.assertEquals(p.<Set>getProperty("set").size(), 4);
    Assert.assertEquals(p.<Map>getProperty("map").size(), 4);
    for (var i = 0; i < 4; i++) {
      var o = p.<List>getProperty("list").get(i);
      Assert.assertTrue(o instanceof Entity);
      Assert.assertEquals(((Entity) o).getProperty("name"), (i + 1) + "");
      o = p.<Map>getProperty("map").get((i + 1) + "");
      Assert.assertTrue(o instanceof Entity);
      Assert.assertEquals(((Entity) o).getProperty("name"), (i + 1) + "");
    }
    for (var o : p.<Set>getProperty("set")) {
      Assert.assertTrue(o instanceof Entity);
      var nameToInt = Integer.parseInt(((Entity) o).getProperty("name"));
      Assert.assertTrue(nameToInt > 0 && nameToInt < 5);
    }

    var other = db.newEntity("JavaSimpleTestClass");
    p.<List>getProperty("list").add(other);
    p.<Set>getProperty("set").add(other);
    p.<Map>getProperty("map").put("5", other);

    db.save(p);
    db.commit();

    db.close();
    db = createSessionInstance();
    db.begin();
    p = db.load(rid);
    Assert.assertEquals(p.<List>getProperty("list").size(), 5);
    var o = p.<List>getProperty("list").get(4);
    Assert.assertTrue(o instanceof Entity);
    o = p.<Map>getProperty("map").get("5");
    Assert.assertTrue(o instanceof Entity);
    var hasOther = false;
    for (var obj : p.<Set>getProperty("set")) {
      hasOther = hasOther || (obj instanceof Entity);
    }
    Assert.assertTrue(hasOther);
    db.commit();
  }

  public void oidentifableFieldsTest() {
    var p = db.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Dean Winchester");

    var testEmbeddedDocument = ((EntityImpl) db.newEntity());
    testEmbeddedDocument.field("testEmbeddedField", "testEmbeddedValue");

    p.setProperty("embeddedDocument", testEmbeddedDocument);

    db.begin();
    var testDocument = ((EntityImpl) db.newEntity());
    testDocument.field("testField", "testValue");

    testDocument.save();
    db.commit();

    db.begin();
    testDocument = db.bindToSession(testDocument);
    p.setProperty("document", testDocument);

    var testRecordBytes =
        db.newBlob(
            "this is a bytearray test. if you read this Object database has stored it correctly"
                .getBytes());

    p.setProperty("byteArray", testRecordBytes);

    db.save(p);
    db.commit();

    var rid = p.getIdentity();

    db.close();

    db = createSessionInstance();
    db.begin();
    Entity loaded = db.load(rid);

    Assert.assertNotNull(loaded.getBlobProperty("byteArray"));
    try {
      try (var out = new ByteArrayOutputStream()) {
        loaded.getBlobProperty("byteArray").toOutputStream(out);
        Assert.assertEquals(
            "this is a bytearray test. if you read this Object database has stored it correctly"
                .getBytes(),
            out.toByteArray());
        Assert.assertEquals(
            out.toString(),
            "this is a bytearray test. if you read this Object database has stored it correctly");
      }
    } catch (IOException ioe) {
      Assert.fail();
      LogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    Assert.assertTrue(loaded.getEntityProperty("document") instanceof EntityImpl);
    Assert.assertEquals(
        loaded.getEntityProperty("document").getProperty("testField"), "testValue");
    Assert.assertTrue(loaded.getEntityProperty("document").getIdentity().isPersistent());

    Assert.assertTrue(loaded.getEntityProperty("embeddedDocument") instanceof EntityImpl);
    Assert.assertEquals(
        loaded.getEntityProperty("embeddedDocument").getProperty("testEmbeddedField"),
        "testEmbeddedValue");
    Assert.assertFalse(
        ((RecordId) loaded.getEntityProperty("embeddedDocument").getIdentity()).isValid());

    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    p = db.newInstance("JavaComplexTestClass");
    var thumbnailImageBytes =
        "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
            .getBytes();
    var oRecordBytes = db.newBlob(thumbnailImageBytes);

    oRecordBytes.save();
    p.setProperty("byteArray", oRecordBytes);

    p = db.save(p);
    db.commit();

    db.begin();
    p = db.bindToSession(p);
    Assert.assertNotNull(p.getBlobProperty("byteArray"));
    try {
      try (var out = new ByteArrayOutputStream()) {
        p.getBlobProperty("byteArray").toOutputStream(out);
        Assert.assertEquals(
            "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
                .getBytes(),
            out.toByteArray());
        Assert.assertEquals(
            out.toString(),
            "this is a bytearray test. if you read this Object database has stored it"
                + " correctlyVERSION2");
      }
    } catch (IOException ioe) {
      Assert.fail();
      LogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    rid = p.getIdentity();

    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);

    Assert.assertNotNull(loaded.getBlobProperty("byteArray"));
    try {
      try (var out = new ByteArrayOutputStream()) {
        loaded.getBlobProperty("byteArray").toOutputStream(out);
        Assert.assertEquals(
            "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
                .getBytes(),
            out.toByteArray());
        Assert.assertEquals(
            out.toString(),
            "this is a bytearray test. if you read this Object database has stored it"
                + " correctlyVERSION2");
      }
    } catch (IOException ioe) {
      Assert.fail();
      LogManager.instance().error(this, "Error reading byte[]", ioe);
      throw new RuntimeException(ioe);
    }
    db.commit();
    db.close();
    db = createSessionInstance();

    db.begin();
    p = db.newInstance("JavaComplexTestClass");
    thumbnailImageBytes =
        "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
            .getBytes();

    oRecordBytes = db.newBlob(thumbnailImageBytes);
    oRecordBytes.save();
    p.setProperty("byteArray", oRecordBytes);

    p = db.save(p);
    db.commit();

    db.begin();
    p = db.bindToSession(p);
    Assert.assertNotNull(p.getBlobProperty("byteArray"));
    try {
      try (var out = new ByteArrayOutputStream()) {
        p.getBlobProperty("byteArray").toOutputStream(out);
        Assert.assertEquals(
            "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
                .getBytes(),
            out.toByteArray());
        Assert.assertEquals(
            out.toString(),
            "this is a bytearray test. if you read this Object database has stored it"
                + " correctlyVERSION2");
      }
    } catch (IOException ioe) {
      Assert.fail();
    }
    rid = p.getIdentity();

    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    loaded = db.load(rid);

    loaded.getBlobProperty("byteArray");
    try {
      try (var out = new ByteArrayOutputStream()) {
        loaded.getBlobProperty("byteArray").toOutputStream(out);
        Assert.assertEquals(
            "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
                .getBytes(),
            out.toByteArray());
        Assert.assertEquals(
            out.toString(),
            "this is a bytearray test. if you read this Object database has stored it"
                + " correctlyVERSION2");
      }
    } catch (IOException ioe) {
      Assert.fail();
      LogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    db.commit();
  }

  @Test
  public void testObjectDelete() {
    var media = db.newEntity("Media");
    var testRecord = db.newBlob("This is a test".getBytes());

    media.setProperty("content", testRecord);

    db.begin();
    media = db.save(media);
    db.commit();

    db.begin();
    media = db.bindToSession(media);
    Assert.assertEquals(new String(media.getBlobProperty("content").toStream()), "This is a test");

    // try to delete
    db.delete(db.bindToSession(media));
    db.commit();
  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void update() {
    var i = new int[]{0};

    db.executeInTxBatches((Iterator<EntityImpl>) db.browseClass("Account"),
        (session, a) -> {
          if (i[0] % 2 == 0) {
            var addresses = a.<List<Identifiable>>getProperty("addresses");
            var newAddress = db.newEntity("Address");

            newAddress.setProperty("street", "Plaza central");
            newAddress.setProperty("type", "work");

            var city = db.newEntity("City");
            city.setProperty("name", "Madrid");

            var country = db.newEntity("Country");
            country.setProperty("name", "Spain");

            city.setProperty("country", country);
            newAddress.setProperty("city", city);

            var newAddresses = new ArrayList<>(addresses);
            newAddresses.addFirst(newAddress);
            a.setProperty("addresses", newAddresses);
          }

          a.setProperty("salary", (i[0] + 500.10f));

          session.save(a);
          i[0]++;
        });
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    var i = 0;
    db.begin();
    Entity a;
    for (var iterator = db.query("select from Account"); iterator.hasNext(); ) {
      a = iterator.next().asEntity();

      if (i % 2 == 0) {
        Assert.assertEquals(
            a.<List<Identifiable>>getProperty("addresses")
                .getFirst()
                .<Entity>getRecord(db)
                .<Identifiable>getProperty("city")
                .<Entity>getRecord(db)
                .<Entity>getRecord(db)
                .<Identifiable>getProperty("country")
                .<Entity>getRecord(db)
                .getProperty("name"),
            "Spain");
      } else {
        Assert.assertEquals(
            a.<List<Identifiable>>getProperty("addresses")
                .getFirst()
                .<Entity>getRecord(db)
                .<Identifiable>getProperty("city")
                .<Entity>getRecord(db)
                .<Entity>getRecord(db)
                .<Identifiable>getProperty("country")
                .<Entity>getRecord(db)
                .getProperty("name"),
            "Italy");
      }

      Assert.assertEquals(a.<Float>getProperty("salary"), i + 500.1f);

      i++;
    }
    db.commit();
  }

  @Test(dependsOnMethods = "testUpdate")
  public void checkLazyLoadingOff() {
    var profiles = db.countClass("Profile");

    db.begin();
    var neo = db.newEntity("Profile");
    neo.setProperty("nick", "Neo");
    neo.setProperty("value", 1);

    var address = db.newEntity("Address");
    address.setProperty("street", "Rio de Castilla");
    address.setProperty("type", "residence");

    var city = db.newEntity("City");
    city.setProperty("name", "Madrid");

    var country = db.newEntity("Country");
    country.setProperty("name", "Spain");

    city.setProperty("country", country);
    address.setProperty("city", city);

    var morpheus = db.newEntity("Profile");
    morpheus.setProperty("nick", "Morpheus");

    var trinity = db.newEntity("Profile");
    trinity.setProperty("nick", "Trinity");

    var followers = new HashSet<>();
    followers.add(trinity);
    followers.add(morpheus);

    neo.setProperty("followers", followers);
    neo.setProperty("location", address);

    db.save(neo);
    db.commit();

    db.begin();
    Assert.assertEquals(db.countClass("Profile"), profiles + 3);

    for (Entity obj : db.browseClass("Profile")) {
      var followersList = obj.<Set<Identifiable>>getProperty("followers");
      Assert.assertTrue(followersList == null || followersList instanceof LinkSet);
      if (obj.<String>getProperty("nick").equals("Neo")) {
        Assert.assertEquals(obj.<Set<Identifiable>>getProperty("followers").size(), 2);
        Assert.assertEquals(
            obj.<Set<Identifiable>>getProperty("followers")
                .iterator()
                .next()
                .getEntity(db)
                .getClassName(),
            "Profile");
      } else if (obj.<String>getProperty("nick").equals("Morpheus")
          || obj.<String>getProperty("nick").equals("Trinity")) {
        Assert.assertNull(obj.<Set<Identifiable>>getProperty("followers"));
      }
    }
    db.commit();
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryPerFloat() {
    db.begin();
    var resultSet = executeQuery("select * from Account where salary = 500.10");

    Assert.assertFalse(resultSet.isEmpty());

    Entity account;
    for (var entries : resultSet) {
      account = entries.asEntity();
      Assert.assertEquals(account.<Float>getProperty("salary"), 500.10f);
    }
    db.commit();
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryCross3Levels() {
    db.begin();
    var resultSet =
        executeQuery("select from Profile where location.city.country.name = 'Spain'");

    Assert.assertFalse(resultSet.isEmpty());

    Entity profile;
    for (var entries : resultSet) {
      profile = entries.asEntity();
      Assert.assertEquals(
          profile
              .getEntityProperty("location")
              .<Entity>getRecord(db)
              .<Identifiable>getProperty("city")
              .<Entity>getRecord(db)
              .<Entity>getRecord(db)
              .<Identifiable>getProperty("country")
              .<Entity>getRecord(db)
              .getProperty("name"),
          "Spain");
    }
    db.commit();
  }

  @Test(dependsOnMethods = "queryCross3Levels")
  public void deleteFirst() {
    startRecordNumber = db.countClass("Account");

    // DELETE ALL THE RECORD IN THE CLASS
    db.forEachInTx((Iterator<EntityImpl>) db.browseClass("Account"),
        ((session, document) -> {
          session.delete(document);
          return false;
        }));

    Assert.assertEquals(db.countClass("Account"), startRecordNumber - 1);
  }

  @Test
  public void commandWithPositionalParameters() {
    db.begin();
    var resultSet =
        executeQuery("select from Profile where name = ? and surname = ?", "Barack", "Obama");

    Assert.assertFalse(resultSet.isEmpty());
    db.commit();
  }

  @Test
  public void queryWithPositionalParameters() {
    db.begin();
    var resultSet =
        executeQuery("select from Profile where name = ? and surname = ?", "Barack", "Obama");

    Assert.assertFalse(resultSet.isEmpty());
    db.commit();
  }

  @Test
  public void queryWithRidAsParameters() {
    db.begin();
    Entity profile = db.browseClass("Profile").next();
    var resultSet =
        executeQuery("select from Profile where @rid = ?", profile.getIdentity());

    Assert.assertEquals(resultSet.size(), 1);
    db.commit();
  }

  @Test
  public void queryWithRidStringAsParameters() {
    db.begin();
    Entity profile = db.browseClass("Profile").next();
    var resultSet =
        executeQuery("select from Profile where @rid = ?", profile.getIdentity());

    Assert.assertEquals(resultSet.size(), 1);
    db.commit();
  }

  @Test
  public void commandWithNamedParameters() {
    addBarackObamaAndFollowers();

    var params = new HashMap<String, String>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    db.begin();
    var resultSet =
        executeQuery("select from Profile where name = :name and surname = :surname", params);
    Assert.assertFalse(resultSet.isEmpty());
    db.commit();
  }

  @Test
  public void commandWithWrongNamedParameters() {
    try {
      var params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      executeQuery("select from Profile where name = :name and surname = :surname%", params);
      Assert.fail();
    } catch (CommandSQLParsingException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void queryConcatAttrib() {
    db.begin();
    Assert.assertFalse(executeQuery("select from City where country.@class = 'Country'").isEmpty());
    Assert.assertEquals(
        executeQuery("select from City where country.@class = 'Country22'").size(), 0);
    db.commit();
  }

  @Test
  public void queryPreparedTwice() {
    try (var db = acquireSession()) {
      db.begin();

      var params = new HashMap<String, String>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      var result =
          db.query("select from Profile where name = :name and surname = :surname", params)
              .entityStream()
              .toList();
      Assert.assertFalse(result.isEmpty());

      result =
          db.query("select from Profile where name = :name and surname = :surname", params)
              .entityStream()
              .toList();
      Assert.assertFalse(result.isEmpty());
      db.commit();
    }
  }

  @Test(dependsOnMethods = "oidentifableFieldsTest")
  public void testEmbeddedDeletion() {
    db.begin();
    var parent = db.newInstance("Parent");
    parent.setProperty("name", "Big Parent");

    var embedded = db.newInstance("EmbeddedChild");
    embedded.setProperty("name", "Little Child");

    parent.setProperty("embeddedChild", embedded);

    parent = db.save(parent);

    var presult = executeQuery("select from Parent");
    var cresult = executeQuery("select from EmbeddedChild");
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 0);

    var child = db.newInstance("EmbeddedChild");
    child.setProperty("name", "Little Child");
    parent.setProperty("child", child);

    parent = db.save(parent);
    db.commit();

    db.begin();
    presult = executeQuery("select from Parent");
    cresult = executeQuery("select from EmbeddedChild");
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 0);

    db.delete(db.bindToSession(parent));
    db.commit();

    db.begin();
    presult = executeQuery("select * from Parent");
    cresult = executeQuery("select * from EmbeddedChild");

    Assert.assertEquals(presult.size(), 0);
    Assert.assertEquals(cresult.size(), 0);
    db.commit();
  }

  @Test(enabled = false, dependsOnMethods = "testCreate")
  public void testEmbeddedBinary() {
    var a = db.newEntity("Account");
    a.setProperty("name", "Chris");
    a.setProperty("surname", "Martin");
    a.setProperty("id", 0);
    a.setProperty("thumbnail", new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    a = db.save(a);
    db.commit();

    db.close();

    db = createSessionInstance();
    Entity aa = db.load(a.getIdentity());
    Assert.assertNotNull(a.getProperty("thumbnail"));
    Assert.assertNotNull(aa.getProperty("thumbnail"));
    byte[] b = aa.getProperty("thumbnail");
    for (var i = 0; i < 10; ++i) {
      Assert.assertEquals(b[i], i);
    }
  }

  @Test
  public void queryById() {
    db.begin();
    var result1 = executeQuery("select from Profile limit 1");
    var result2 =
        executeQuery("select from Profile where @rid = ?", result1.getFirst().getIdentity());

    Assert.assertFalse(result2.isEmpty());
    db.commit();
  }

  @Test
  public void queryByIdNewApi() {
    db.begin();
    db.command("insert into Profile set nick = 'foo', name='foo'").close();
    db.commit();

    db.begin();
    var result1 = executeQuery("select from Profile where nick = 'foo'");

    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(result1.getFirst().asEntity().getClassName(), "Profile");
    var profile = result1.getFirst().asEntity();

    Assert.assertEquals(profile.getProperty("nick"), "foo");
    db.commit();
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testSaveMultiCircular() {
    db = createSessionInstance();
    try {
      startRecordNumber = db.countClusterElements("Profile");
      db.begin();
      var bObama = db.newInstance("Profile");
      bObama.setProperty("nick", "TheUSPresident");
      bObama.setProperty("name", "Barack");
      bObama.setProperty("surname", "Obama");

      var address = db.newInstance("Address");
      address.setProperty("type", "Residence");

      var city = db.newInstance("City");
      city.setProperty("name", "Washington");

      var country = db.newInstance("Country");
      country.setProperty("name", "USA");

      city.setProperty("country", country);
      address.setProperty("city", city);

      bObama.setProperty("location", address);

      var presidentSon1 = db.newInstance("Profile");
      presidentSon1.setProperty("nick", "PresidentSon10");
      presidentSon1.setProperty("name", "Malia Ann");
      presidentSon1.setProperty("surname", "Obama");
      presidentSon1.setProperty("invitedBy", bObama);

      var presidentSon2 = db.newInstance("Profile");
      presidentSon2.setProperty("nick", "PresidentSon20");
      presidentSon2.setProperty("name", "Natasha");
      presidentSon2.setProperty("surname", "Obama");
      presidentSon2.setProperty("invitedBy", bObama);

      var followers = new ArrayList<>();
      followers.add(presidentSon1);
      followers.add(presidentSon2);

      bObama.setProperty("followers", followers);

      db.save(bObama);
      db.commit();
    } finally {
      db.close();
    }
  }

  private void createSimpleArrayTestClass() {
    if (db.getSchema().existsClass("JavaSimpleArrayTestClass")) {
      db.getSchema().dropClass("JavaSimpleSimpleArrayTestClass");
    }

    var cls = db.createClass("JavaSimpleArrayTestClass");
    cls.createProperty(db, "text", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "numberSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "longSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "doubleSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "floatSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "byteSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "flagSimple", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "dateField", PropertyType.EMBEDDEDLIST);
  }

  private void createBinaryTestClass() {
    if (db.getSchema().existsClass("JavaBinaryTestClass")) {
      db.getSchema().dropClass("JavaBinaryTestClass");
    }

    var cls = db.createClass("JavaBinaryTestClass");
    cls.createProperty(db, "binaryData", PropertyType.BINARY);
  }

  private void createPersonClass() {
    if (db.getClass("PersonTest") == null) {
      var cls = db.createClass("PersonTest");
      cls.createProperty(db, "firstname", PropertyType.STRING);
      cls.createProperty(db, "friends", PropertyType.LINKSET);
    }
  }

  private void createEventClass() {
    if (db.getClass("Event") == null) {
      var cls = db.createClass("Event");
      cls.createProperty(db, "name", PropertyType.STRING);
      cls.createProperty(db, "date", PropertyType.DATE);
    }
  }

  private void createAgendaClass() {
    if (db.getClass("Agenda") == null) {
      var cls = db.createClass("Agenda");
      cls.createProperty(db, "events", PropertyType.EMBEDDEDLIST);
    }
  }

  private void createNonGenericClass() {
    if (db.getClass("JavaNoGenericCollectionsTestClass") == null) {
      var cls = db.createClass("JavaNoGenericCollectionsTestClass");
      cls.createProperty(db, "list", PropertyType.EMBEDDEDLIST);
      cls.createProperty(db, "set", PropertyType.EMBEDDEDSET);
      cls.createProperty(db, "map", PropertyType.EMBEDDEDMAP);
    }
  }

  private void createMediaClass() {
    if (db.getClass("Media") == null) {
      var cls = db.createClass("Media");
      cls.createProperty(db, "content", PropertyType.LINK);
      cls.createProperty(db, "name", PropertyType.STRING);
    }
  }

  private void createParentChildClasses() {
    if (db.getSchema().existsClass("Parent")) {
      db.getSchema().dropClass("Parent");
    }
    if (db.getSchema().existsClass("EmbeddedChild")) {
      db.getSchema().dropClass("EmbeddedChild");
    }

    var parentCls = db.createClass("Parent");
    parentCls.createProperty(db, "name", PropertyType.STRING);
    parentCls.createProperty(db, "child", PropertyType.EMBEDDED,
        db.getClass("EmbeddedChild"));
    parentCls.createProperty(db, "embeddedChild", PropertyType.EMBEDDED,
        db.getClass("EmbeddedChild"));

    var childCls = db.createClass("EmbeddedChild");
    childCls.createProperty(db, "name", PropertyType.STRING);
  }
}
