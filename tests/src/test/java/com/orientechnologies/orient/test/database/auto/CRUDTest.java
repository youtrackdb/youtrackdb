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

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
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
public class CRUDTest extends DocumentDBBaseTest {

  protected long startRecordNumber;

  private Entity rome;

  @Parameters(value = "remote")
  public CRUDTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
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
    startRecordNumber = database.countClass("Account");

    Entity address;

    database.begin();
    var country = database.newEntity("Country");
    country.setProperty("name", "Italy");
    country.save();

    rome = database.newEntity("City");
    rome.setProperty("name", "Rome");
    rome.setProperty("country", country);
    database.save(rome);

    address = database.newEntity("Address");
    address.setProperty("type", "Residence");
    address.setProperty("street", "Piazza Navona, 1");
    address.setProperty("city", rome);
    database.save(address);

    for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS_ACCOUNT; ++i) {
      Entity account = database.newEntity("Account");
      account.setProperty("id", i);
      account.setProperty("name", "Bill");
      account.setProperty("surname", "Gates");
      account.setProperty("birthDate", new Date());
      account.setProperty("salary", (i + 300.10f));
      account.setProperty("addresses", Collections.singletonList(address));
      database.save(account);
    }
    database.commit();
  }

  @Test(dependsOnMethods = "create")
  public void testCreate() {
    Assert.assertEquals(database.countClass("Account") - startRecordNumber, TOT_RECORDS_ACCOUNT);
  }

  @Test(dependsOnMethods = "testCreate")
  public void testCreateClass() {
    var schema = database.getMetadata().getSchema();
    Assert.assertNull(schema.getClass("Dummy"));
    var dummyClass = schema.createClass("Dummy");
    dummyClass.createProperty(database, "name", YTType.STRING);

    Assert.assertEquals(database.countClass("Dummy"), 0);
    Assert.assertNotNull(schema.getClass("Dummy"));
  }

  @Test
  public void testSimpleTypes() {
    Entity element = database.newEntity("JavaSimpleTestClass");
    Assert.assertEquals(element.getProperty("text"), "initTest");

    database.begin();
    Date date = new Date();
    element.setProperty("text", "test");
    element.setProperty("numberSimple", 12345);
    element.setProperty("doubleSimple", 12.34d);
    element.setProperty("floatSimple", 123.45f);
    element.setProperty("longSimple", 12345678L);
    element.setProperty("byteSimple", (byte) 1);
    element.setProperty("flagSimple", true);
    element.setProperty("dateField", date);

    database.save(element);
    database.commit();

    YTRID id = element.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    EntityImpl loadedRecord = database.load(id);
    Assert.assertEquals(loadedRecord.getProperty("text"), "test");
    Assert.assertEquals(loadedRecord.<Integer>getProperty("numberSimple"), 12345);
    Assert.assertEquals(loadedRecord.<Double>getProperty("doubleSimple"), 12.34d);
    Assert.assertEquals(loadedRecord.<Float>getProperty("floatSimple"), 123.45f);
    Assert.assertEquals(loadedRecord.<Long>getProperty("longSimple"), 12345678L);
    Assert.assertEquals(loadedRecord.<Byte>getProperty("byteSimple"), (byte) 1);
    Assert.assertEquals(loadedRecord.<Boolean>getProperty("flagSimple"), true);
    Assert.assertEquals(loadedRecord.getProperty("dateField"), date);
    database.commit();
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testSimpleArrayTypes() {
    Entity element = database.newInstance("JavaSimpleArraysTestClass");
    String[] textArray = new String[10];
    int[] intArray = new int[10];
    long[] longArray = new long[10];
    double[] doubleArray = new double[10];
    float[] floatArray = new float[10];
    boolean[] booleanArray = new boolean[10];
    Date[] dateArray = new Date[10];
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.YEAR, 1900);
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    for (int i = 0; i < 10; i++) {
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

    database.begin();
    database.save(element);
    database.commit();
    YTRID id = element.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loadedElement = database.load(id);
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

    for (int i = 0; i < 10; i++) {
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

    for (int i = 0; i < 10; i++) {
      int j = i + 10;
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

    database.save(loadedElement);
    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    loadedElement = database.load(id);
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

    for (int i = 0; i < 10; i++) {
      int j = i + 10;
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

    database.commit();
    database.close();

    database = createSessionInstance();

    database.begin();
    loadedElement = database.load(id);

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

    database.delete(id);
    database.commit();
  }

  @Test(dependsOnMethods = "testSimpleTypes")
  public void testBinaryDataType() {
    Entity element = database.newInstance("JavaBinaryDataTestClass");
    byte[] bytes = new byte[10];
    for (int i = 0; i < 10; i++) {
      bytes[i] = (byte) i;
    }

    element.setProperty("binaryData", bytes);

    String fieldName = "binaryData";
    Assert.assertNotNull(element.getProperty(fieldName));

    database.begin();
    database.save(element);
    database.commit();

    YTRID id = element.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loadedElement = database.load(id);
    Assert.assertNotNull(loadedElement.getProperty(fieldName));

    Assert.assertEquals(loadedElement.<byte[]>getProperty("binaryData").length, 10);
    Assert.assertEquals(loadedElement.getProperty("binaryData"), bytes);

    for (int i = 0; i < 10; i++) {
      int j = i + 10;
      bytes[i] = (byte) j;
    }
    loadedElement.setProperty("binaryData", bytes);

    database.save(loadedElement);
    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    loadedElement = database.load(id);
    Assert.assertNotNull(loadedElement.getProperty(fieldName));

    Assert.assertEquals(loadedElement.<byte[]>getProperty("binaryData").length, 10);
    Assert.assertEquals(loadedElement.getProperty("binaryData"), bytes);

    database.commit();
    database.close();

    database = createSessionInstance();

    database.begin();
    database.delete(id);
    database.commit();
  }

  @Test(dependsOnMethods = "testSimpleArrayTypes")
  public void collectionsDocumentTypeTestPhaseOne() {
    database.begin();
    Entity a = database.newInstance("JavaComplexTestClass");

    for (int i = 0; i < 3; i++) {
      var child1 = database.newEntity("Child");
      var child2 = database.newEntity("Child");
      var child3 = database.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }

    a = database.save(a);
    database.commit();

    YTRID rid = a.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    List<EntityImpl> agendas = executeQuery("SELECT FROM " + rid);

    EntityImpl testLoadedEntity = agendas.get(0);

    checkCollectionImplementations(testLoadedEntity);

    database.save(testLoadedEntity);
    database.commit();

    database.freeze(false);
    database.release();

    database.begin();

    testLoadedEntity = database.load(rid);

    checkCollectionImplementations(testLoadedEntity);
    database.commit();
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseOne")
  public void collectionsDocumentTypeTestPhaseTwo() {
    database.begin();
    Entity a = database.newInstance("JavaComplexTestClass");

    for (int i = 0; i < 10; i++) {
      var child1 = database.newEntity("Child");
      var child2 = database.newEntity("Child");
      var child3 = database.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }

    a = database.save(a);
    database.commit();

    YTRID rid = a.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    List<EntityImpl> agendas = executeQuery("SELECT FROM " + rid);
    var testLoadedEntity = agendas.get(0);

    checkCollectionImplementations(testLoadedEntity);

    testLoadedEntity = database.save(testLoadedEntity);
    database.commit();

    database.freeze(false);
    database.release();

    database.begin();
    checkCollectionImplementations(database.bindToSession(testLoadedEntity));
    database.commit();
  }

  @Test(dependsOnMethods = "collectionsDocumentTypeTestPhaseTwo")
  public void collectionsDocumentTypeTestPhaseThree() {
    Entity a = database.newInstance("JavaComplexTestClass");

    database.begin();
    for (int i = 0; i < 100; i++) {
      var child1 = database.newEntity("Child");
      var child2 = database.newEntity("Child");
      var child3 = database.newEntity("Child");

      a.setProperty("list", Collections.singletonList(child1));
      a.setProperty("set", Collections.singleton(child2));
      a.setProperty("children", Collections.singletonMap("" + i, child3));
    }
    a = database.save(a);
    database.commit();

    YTRID rid = a.getIdentity();
    database.close();

    database = createSessionInstance();
    database.begin();
    List<EntityImpl> agendas = executeQuery("SELECT FROM " + rid);
    var testLoadedEntity = agendas.get(0);
    checkCollectionImplementations(testLoadedEntity);

    testLoadedEntity = database.save(testLoadedEntity);
    database.commit();

    database.freeze(false);
    database.release();

    database.begin();
    checkCollectionImplementations(database.bindToSession(testLoadedEntity));
    database.rollback();
  }

  protected static void checkCollectionImplementations(EntityImpl doc) {
    Object collectionObj = doc.field("list");
    boolean validImplementation =
        (collectionObj instanceof TrackedList<?>) || (doc.field("list") instanceof LinkList);
    if (!validImplementation) {
      Assert.fail(
          "Document list implementation "
              + collectionObj.getClass().getName()
              + " not compatible with current Object Database loading management");
    }
    collectionObj = doc.field("set");
    validImplementation =
        (collectionObj instanceof TrackedSet<?>) || (collectionObj instanceof LinkSet);
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
    var element = database.newEntity("JavaSimpleTestClass");
    Date date = new Date();
    element.setProperty("dateField", date);
    database.begin();
    element.save();
    database.commit();

    database.begin();
    element = database.bindToSession(element);
    Assert.assertEquals(element.<List<Date>>getProperty("dateField"), date);
    database.commit();
  }

  @Test(dependsOnMethods = "testCreateClass")
  public void readAndBrowseDescendingAndCheckHoleUtilization() {
    database.begin();
    rome = database.bindToSession(rome);
    Set<Integer> ids = new HashSet<>(TOT_RECORDS_ACCOUNT);
    for (int i = 0; i < TOT_RECORDS_ACCOUNT; i++) {
      ids.add(i);
    }

    for (Entity a : database.browseClass("Account")) {
      int id = a.<Integer>getProperty("id");
      Assert.assertTrue(ids.remove(id));

      Assert.assertEquals(a.<Integer>getProperty("id"), id);
      Assert.assertEquals(a.getProperty("name"), "Bill");
      Assert.assertEquals(a.getProperty("surname"), "Gates");
      Assert.assertEquals(a.<Float>getProperty("salary"), id + 300.1f);
      Assert.assertEquals(a.<List<YTIdentifiable>>getProperty("addresses").size(), 1);
      Assert.assertEquals(
          a.<List<YTIdentifiable>>getProperty("addresses")
              .get(0)
              .<Entity>getRecord()
              .getElementProperty("city")
              .getProperty("name"),
          rome.<String>getProperty("name"));
      Assert.assertEquals(
          a.<List<YTIdentifiable>>getProperty("addresses")
              .get(0)
              .<Entity>getRecord()
              .getElementProperty("city")
              .getElementProperty("country")
              .getProperty("name"),
          rome.<Entity>getRecord()
              .<YTIdentifiable>getProperty("country")
              .<Entity>getRecord()
              .<String>getProperty("name"));
    }

    Assert.assertTrue(ids.isEmpty());
    database.commit();
  }

  @Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
  public void mapEnumAndInternalObjects() {
    database.executeInTxBatches((Iterator<EntityImpl>) database.browseClass("OUser"),
        ((session, document) -> {
          document.save();
        }));

  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void mapObjectsLinkTest() {
    var p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    var c = database.newInstance("Child");
    c.setProperty("name", "John");

    var c1 = database.newInstance("Child");
    c1.setProperty("name", "Jack");

    var c2 = database.newInstance("Child");
    c2.setProperty("name", "Bob");

    var c3 = database.newInstance("Child");
    c3.setProperty("name", "Sam");

    var c4 = database.newInstance("Child");
    c4.setProperty("name", "Dean");

    var list = new ArrayList<YTIdentifiable>();
    list.add(c1);
    list.add(c2);
    list.add(c3);
    list.add(c4);

    p.setProperty("list", list);

    var children = new HashMap<String, Entity>();
    children.put("first", c);
    p.setProperty("children", children);

    database.begin();
    database.save(p);
    database.commit();

    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");

    Assert.assertFalse(cresult.isEmpty());

    YTRID rid = p.getIdentity();
    database.commit();

    database.close();

    database = createSessionInstance();
    database.begin();
    var loaded = database.<Entity>load(rid);

    list = loaded.getProperty("list");
    Assert.assertEquals(list.size(), 4);
    Assert.assertEquals(
        Objects.requireNonNull(list.get(0).<Entity>getRecord().getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(1).<Entity>getRecord().getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(2).<Entity>getRecord().getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(list.get(3).<Entity>getRecord().getSchemaClass()).getName(),
        "Child");
    Assert.assertEquals(list.get(0).<Entity>getRecord().getProperty("name"), "Jack");
    Assert.assertEquals(list.get(1).<Entity>getRecord().getProperty("name"), "Bob");
    Assert.assertEquals(list.get(2).<Entity>getRecord().getProperty("name"), "Sam");
    Assert.assertEquals(list.get(3).<Entity>getRecord().getProperty("name"), "Dean");
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void listObjectsLinkTest() {
    database.begin();
    var hanSolo = database.newInstance("PersonTest");
    hanSolo.setProperty("firstName", "Han");
    hanSolo = database.save(hanSolo);
    database.commit();

    database.begin();
    var obiWan = database.newInstance("PersonTest");
    obiWan.setProperty("firstName", "Obi-Wan");
    obiWan = database.save(obiWan);

    var luke = database.newInstance("PersonTest");
    luke.setProperty("firstName", "Luke");
    luke = database.save(luke);
    database.commit();

    // ============================== step 1
    // add new information to luke
    database.begin();
    luke = database.bindToSession(luke);
    var friends = new HashSet<YTIdentifiable>();
    friends.add(database.bindToSession(hanSolo));

    luke.setProperty("friends", friends);
    database.save(luke);
    database.commit();

    database.begin();
    luke = database.bindToSession(luke);
    Assert.assertEquals(luke.<Set<YTIdentifiable>>getProperty("friends").size(), 1);
    friends = new HashSet<>();
    friends.add(database.bindToSession(obiWan));
    luke.setProperty("friends", friends);

    database.save(database.bindToSession(luke));
    database.commit();

    database.begin();
    luke = database.bindToSession(luke);
    Assert.assertEquals(luke.<Set<YTIdentifiable>>getProperty("friends").size(), 1);
    database.commit();
    // ============================== end 2
  }

  @Test(dependsOnMethods = "listObjectsLinkTest")
  public void listObjectsIterationTest() {
    var a = database.newInstance("Agenda");

    for (int i = 0; i < 10; i++) {
      a.setProperty("events", Collections.singletonList(database.newInstance("Event")));
    }
    database.begin();
    a = database.save(a);
    database.commit();
    YTRID rid = a.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    List<EntityImpl> agendas = executeQuery("SELECT FROM " + rid);
    Entity agenda = agendas.get(0);
    //noinspection unused,StatementWithEmptyBody
    for (var e : agenda.<List<Entity>>getProperty("events")) {
      // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
    }

    agenda = database.save(agenda);
    database.commit();

    database.freeze(false);
    database.release();

    database.begin();
    agenda = database.bindToSession(agenda);
    try {
      for (int i = 0; i < agenda.<List<Entity>>getProperty("events").size(); i++) {
        @SuppressWarnings("unused")
        var e = agenda.<List<Entity>>getProperty("events").get(i);
        // NO NEED TO DO ANYTHING, JUST NEED TO ITERATE THE LIST
      }
    } catch (ConcurrentModificationException cme) {
      Assert.fail("Error iterating Object list", cme);
    }

    if (database.getTransaction().isActive()) {
      database.rollback();
    }
  }

  @Test(dependsOnMethods = "listObjectsIterationTest")
  public void mapObjectsListEmbeddedTest() {
    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");

    int childSize = cresult.size();

    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    Entity c = database.newInstance("Child");
    c.setProperty("name", "John");

    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "Jack");

    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Bob");

    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Sam");

    Entity c4 = database.newInstance("Child");
    c4.setProperty("name", "Dean");

    var list = new ArrayList<YTIdentifiable>();
    list.add(c1);
    list.add(c2);
    list.add(c3);
    list.add(c4);

    p.setProperty("embeddedList", list);

    database.save(p);
    database.commit();

    database.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    YTRID rid = p.getIdentity();
    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

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
                    .<Entity>getRecord()
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(1)
                    .<Entity>getRecord()
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(2)
                    .getEntity()
                    .getSchemaClass())
            .getName(),
        "Child");
    Assert.assertEquals(
        Objects.requireNonNull(
                loaded
                    .<List<Entity>>getProperty("embeddedList")
                    .get(3)
                    .getEntity()
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
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsListEmbeddedTest")
  public void mapObjectsSetEmbeddedTest() {
    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");

    int childSize = cresult.size();

    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    Entity c = database.newInstance("Child");
    c.setProperty("name", "John");

    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "Jack");

    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Bob");

    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Sam");

    Entity c4 = database.newInstance("Child");
    c4.setProperty("name", "Dean");

    var embeddedSet = new HashSet<Entity>();
    embeddedSet.add(c);
    embeddedSet.add(c1);
    embeddedSet.add(c2);
    embeddedSet.add(c3);
    embeddedSet.add(c4);

    p.setProperty("embeddedSet", embeddedSet);

    database.save(p);
    database.commit();

    database.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    YTRID rid = p.getIdentity();
    database.commit();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    Assert.assertEquals(loaded.<Set<Entity>>getProperty("embeddedSet").size(), 5);
    for (Entity loadedC : loaded.<Set<Entity>>getProperty("embeddedSet")) {
      Assert.assertTrue(loadedC.isEmbedded());
      Assert.assertEquals(loadedC.getClassName(), "Child");
      Assert.assertTrue(
          loadedC.<String>getProperty("name").equals("John")
              || loadedC.<String>getProperty("name").equals("Jack")
              || loadedC.<String>getProperty("name").equals("Bob")
              || loadedC.<String>getProperty("name").equals("Sam")
              || loadedC.<String>getProperty("name").equals("Dean"));
    }
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsSetEmbeddedTest")
  public void mapObjectsMapEmbeddedTest() {
    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");

    int childSize = cresult.size();

    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    Entity c = database.newInstance("Child");
    c.setProperty("name", "John");

    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "Jack");

    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Bob");

    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Sam");

    Entity c4 = database.newInstance("Child");
    c4.setProperty("name", "Dean");

    var embeddedChildren = new HashMap<String, Entity>();
    embeddedChildren.put(c.getProperty("name"), c);
    embeddedChildren.put(c1.getProperty("name"), c1);
    embeddedChildren.put(c2.getProperty("name"), c2);
    embeddedChildren.put(c3.getProperty("name"), c3);
    embeddedChildren.put(c4.getProperty("name"), c4);

    p.setProperty("embeddedChildren", embeddedChildren);

    database.save(p);
    database.commit();

    database.begin();
    cresult = executeQuery("select * from Child");

    Assert.assertEquals(childSize, cresult.size());

    YTRID rid = p.getIdentity();
    database.commit();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    Assert.assertEquals(loaded.<Map<String, Entity>>getProperty("embeddedChildren").size(), 5);
    for (String key : loaded.<Map<String, Entity>>getProperty("embeddedChildren").keySet()) {
      Entity loadedC = loaded.<Map<String, Entity>>getProperty("embeddedChildren").get(key);
      Assert.assertTrue(loadedC.isEmbedded());
      Assert.assertEquals(loadedC.getClassName(), "Child");
      Assert.assertTrue(
          loadedC.<String>getProperty("name").equals("John")
              || loadedC.<String>getProperty("name").equals("Jack")
              || loadedC.<String>getProperty("name").equals("Bob")
              || loadedC.<String>getProperty("name").equals("Sam")
              || loadedC.<String>getProperty("name").equals("Dean"));
    }
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsNonExistingKeyTest() {
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    database.begin();
    p = database.save(p);

    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "John");

    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Jack");

    var children = new HashMap<String, Entity>();
    children.put("first", c1);
    children.put("second", c2);

    p.setProperty("children", children);

    database.save(p);
    database.commit();

    database.begin();
    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Olivia");
    Entity c4 = database.newInstance("Child");
    c4.setProperty("name", "Peter");

    p = database.bindToSession(p);
    p.<Map<String, YTIdentifiable>>getProperty("children").put("third", c3);
    p.<Map<String, YTIdentifiable>>getProperty("children").put("fourth", c4);

    database.save(p);
    database.commit();

    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");

    Assert.assertFalse(cresult.isEmpty());

    YTRID rid = p.getIdentity();
    database.commit();

    database.close();

    database = createSessionInstance();
    database.begin();
    c1 = database.bindToSession(c1);
    c2 = database.bindToSession(c2);
    c3 = database.bindToSession(c3);
    c4 = database.bindToSession(c4);

    Entity loaded = database.load(rid);

    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("first")
            .getEntity()
            .getProperty("name"),
        c1.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("second")
            .getEntity()
            .getProperty("name"),
        c2.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("third")
            .getEntity()
            .getProperty("name"),
        c3.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("fourth")
            .getEntity()
            .getProperty("name"),
        c4.<String>getProperty("name"));
    Assert.assertNull(loaded.<Map<String, YTIdentifiable>>getProperty("children").get("fifth"));
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkTwoSaveTest() {
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Silvester");

    database.begin();
    p = database.save(p);

    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "John");

    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Jack");

    var children = new HashMap<String, YTIdentifiable>();
    children.put("first", c1);
    children.put("second", c2);

    p.setProperty("children", children);

    database.save(p);

    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Olivia");
    Entity c4 = database.newInstance("Child");
    c4.setProperty("name", "Peter");

    p.<Map<String, YTIdentifiable>>getProperty("children").put("third", c3);
    p.<Map<String, YTIdentifiable>>getProperty("children").put("fourth", c4);

    database.save(p);
    database.commit();

    database.begin();
    List<EntityImpl> cresult = executeQuery("select * from Child");
    Assert.assertFalse(cresult.isEmpty());

    YTRID rid = p.getIdentity();
    database.commit();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    c1 = database.bindToSession(c1);
    c2 = database.bindToSession(c2);
    c3 = database.bindToSession(c3);
    c4 = database.bindToSession(c4);

    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("first")
            .getEntity()
            .getProperty("name"),
        c1.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("second")
            .getEntity()
            .getProperty("name"),
        c2.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("third")
            .getEntity()
            .getProperty("name"),
        c3.<String>getProperty("name"));
    Assert.assertEquals(
        loaded
            .<Map<String, YTIdentifiable>>getProperty("children")
            .get("fourth")
            .getEntity()
            .getProperty("name"),
        c4.<String>getProperty("name"));
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkTest")
  public void mapObjectsLinkUpdateDatabaseNewInstanceTest() {
    // TEST WITH NEW INSTANCE
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Fringe");

    Entity c = database.newInstance("Child");
    c.setProperty("name", "Peter");
    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "Walter");
    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Olivia");
    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Astrid");

    Map<String, YTIdentifiable> children = new HashMap<>();
    children.put(c.getProperty("name"), c);
    children.put(c1.getProperty("name"), c1);
    children.put(c2.getProperty("name"), c2);
    children.put(c3.getProperty("name"), c3);

    p.setProperty("children", children);

    database.begin();
    database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    for (String key : loaded.<Map<String, YTIdentifiable>>getProperty("children").keySet()) {
      Assert.assertTrue(
          key.equals("Peter")
              || key.equals("Walter")
              || key.equals("Olivia")
              || key.equals("Astrid"));
      Assert.assertEquals(
          loaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get(key)
              .getEntity()
              .getClassName(),
          "Child");
      Assert.assertEquals(
          key,
          loaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get(key)
              .getEntity()
              .getProperty("name"));
      switch (key) {
        case "Peter" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Peter");
        case "Walter" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Walter");
        case "Olivia" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Olivia");
        case "Astrid" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Astrid");
      }
    }
    database.commit();

    database.begin();
    for (Entity reloaded : database.browseClass("JavaComplexTestClass")) {
      reloaded = database.bindToSession(reloaded);
      Entity c4 = database.newInstance("Child");
      c4.setProperty("name", "The Observer");

      children = reloaded.getProperty("children");
      if (children == null) {
        children = new HashMap<>();
        reloaded.setProperty("children", children);
      }

      children.put(c4.getProperty("name"), c4);

      database.save(reloaded);
    }
    database.commit();

    database.close();
    database = createSessionInstance();
    database.begin();
    for (Entity reloaded : database.browseClass("JavaComplexTestClass")) {
      Assert.assertTrue(
          reloaded.<Map<String, YTIdentifiable>>getProperty("children")
              .containsKey("The Observer"));
      Assert.assertNotNull(
          reloaded.<Map<String, YTIdentifiable>>getProperty("children").get("The Observer"));
      Assert.assertEquals(
          reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getEntity()
              .getProperty("name"),
          "The Observer");
      Assert.assertTrue(
          reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isPersistent()
              && reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isValid());
    }
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateDatabaseNewInstanceTest")
  public void mapObjectsLinkUpdateJavaNewInstanceTest() {
    // TEST WITH NEW INSTANCE
    database.begin();
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Fringe");

    Entity c = database.newInstance("Child");
    c.setProperty("name", "Peter");
    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "Walter");
    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "Olivia");
    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "Astrid");

    var children = new HashMap<String, YTIdentifiable>();
    children.put(c.getProperty("name"), c);
    children.put(c1.getProperty("name"), c1);
    children.put(c2.getProperty("name"), c2);
    children.put(c3.getProperty("name"), c3);

    p.setProperty("children", children);

    p = database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    for (String key : loaded.<Map<String, YTIdentifiable>>getProperty("children").keySet()) {
      Assert.assertTrue(
          key.equals("Peter")
              || key.equals("Walter")
              || key.equals("Olivia")
              || key.equals("Astrid"));
      Assert.assertEquals(
          loaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get(key)
              .getEntity()
              .getClassName(),
          "Child");
      Assert.assertEquals(
          key,
          loaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get(key)
              .getEntity()
              .getProperty("name"));
      switch (key) {
        case "Peter" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Peter");
        case "Walter" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Walter");
        case "Olivia" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Olivia");
        case "Astrid" -> Assert.assertEquals(
            loaded
                .<Map<String, YTIdentifiable>>getProperty("children")
                .get(key)
                .getEntity()
                .getProperty("name"),
            "Astrid");
      }
    }

    for (Entity reloaded : database.browseClass("JavaComplexTestClass")) {
      Entity c4 = database.newInstance("Child");
      c4.setProperty("name", "The Observer");

      reloaded.<Map<String, YTIdentifiable>>getProperty("children").put(c4.getProperty("name"), c4);

      database.save(reloaded);
    }
    database.commit();

    database.close();
    database = createSessionInstance();
    database.begin();
    for (Entity reloaded : database.browseClass("JavaComplexTestClass")) {
      Assert.assertTrue(
          reloaded.<Map<String, YTIdentifiable>>getProperty("children")
              .containsKey("The Observer"));
      Assert.assertNotNull(
          reloaded.<Map<String, YTIdentifiable>>getProperty("children").get("The Observer"));
      Assert.assertEquals(
          reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getEntity()
              .getProperty("name"),
          "The Observer");
      Assert.assertTrue(
          reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isPersistent()
              && reloaded
              .<Map<String, YTIdentifiable>>getProperty("children")
              .get("The Observer")
              .getIdentity()
              .isValid());
    }
    database.commit();
  }

  @Test(dependsOnMethods = "mapObjectsLinkUpdateJavaNewInstanceTest")
  public void mapStringTest() {
    Map<String, String> relatives = new HashMap<>();
    relatives.put("father", "Mike");
    relatives.put("mother", "Julia");

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND HANDLER MANAGEMENT
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    var stringMap = new HashMap<String, String>();
    stringMap.put("father", "Mike");
    stringMap.put("mother", "Julia");

    p.setProperty("stringMap", stringMap);

    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    database.begin();
    database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    database.save(loaded);
    database.commit();

    database.begin();
    loaded = database.bindToSession(loaded);
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    database.commit();
    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    database.begin();
    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringMap", relatives);

    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    database.save(loaded);
    database.commit();

    database.begin();
    for (Entry<String, String> entry : relatives.entrySet()) {
      loaded = database.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    database.begin();
    // TEST WITH JAVA CONSTRUCTOR
    p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringMap", relatives);

    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }

    p = database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    loaded.<Map<String, String>>getProperty("stringMap").put("brother", "Nike");
    relatives.put("brother", "Nike");

    database.save(loaded);
    database.commit();

    database.begin();
    for (Entry<String, String> entry : relatives.entrySet()) {
      loaded = database.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, String>>getProperty("stringMap"));
    for (Entry<String, String> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, String>>getProperty("stringMap").get(entry.getKey()));
    }
    database.delete(database.bindToSession(loaded));
    database.commit();
  }

  @Test(dependsOnMethods = "mapStringTest")
  public void setStringTest() {
    database.begin();
    Entity testClass = database.newInstance("JavaComplexTestClass");
    Set<String> roles = new HashSet<>();

    roles.add("manager");
    roles.add("developer");
    testClass.setProperty("stringSet", roles);

    Entity testClassProxy = database.save(testClass);
    database.commit();

    database.begin();
    testClassProxy = database.bindToSession(testClassProxy);
    Assert.assertEquals(roles.size(), testClassProxy.<Set<String>>getProperty("stringSet").size());
    for (String referenceRole : roles) {
      testClassProxy = database.bindToSession(testClassProxy);
      Assert.assertTrue(
          testClassProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    YTRID orid = testClassProxy.getIdentity();
    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    Entity loadedProxy = database.load(orid);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    database.save(database.bindToSession(loadedProxy));
    database.commit();

    database.begin();
    loadedProxy = database.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }

    loadedProxy.<Set<String>>getProperty("stringSet").remove("developer");
    roles.remove("developer");
    database.save(loadedProxy);
    database.commit();

    database.begin();
    loadedProxy = database.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }
    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    loadedProxy = database.bindToSession(loadedProxy);
    Assert.assertEquals(roles.size(), loadedProxy.<Set<String>>getProperty("stringSet").size());
    for (String referenceRole : roles) {
      Assert.assertTrue(loadedProxy.<Set<String>>getProperty("stringSet").contains(referenceRole));
    }
    database.commit();
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
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    p.setProperty("stringListMap", songAndMovies);

    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.begin();
    database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();
    database.close();
    database = createSessionInstance();

    database.begin();
    Entity loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    p = database.newInstance("JavaComplexTestClass");
    database.begin();
    p.setProperty("name", "Chuck");
    p.setProperty("stringListMap", songAndMovies);

    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    database.begin();
    // TEST WITH OBJECT DATABASE NEW INSTANCE LIST DIRECT ADD
    p = database.newInstance("JavaComplexTestClass");
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

    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    // TEST WITH JAVA CONSTRUCTOR
    database.begin();
    p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("stringListMap", songAndMovies);

    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          p.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    p = database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, List<String>>>getProperty("stringListMap"));
    for (Entry<String, List<String>> entry : songAndMovies.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, List<String>>>getProperty("stringListMap").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();
  }

  @Test
  public void embeddedMapObjectTest() {
    Calendar cal = Calendar.getInstance();
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
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");

    var mapObject = new HashMap<String, Object>();
    mapObject.put("father", "Mike");
    mapObject.put("mother", "Julia");
    mapObject.put("number", 10);
    mapObject.put("date", cal.getTime());

    p.setProperty("mapObject", mapObject);

    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    database.begin();
    database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");
    relatives.put("brother", "Nike");

    database.save(loaded);
    database.commit();

    database.begin();
    for (Entry<String, Object> entry : relatives.entrySet()) {
      loaded = database.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    database.commit();

    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();

    // TEST WITH OBJECT DATABASE NEW INSTANCE AND MAP DIRECT SET
    database.begin();
    p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("mapObject", relatives);

    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();

    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");
    relatives.put("brother", "Nike");

    database.save(loaded);
    database.commit();

    database.begin();
    for (Entry<String, Object> entry : relatives.entrySet()) {
      loaded = database.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    database.commit();

    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();
    database.begin();

    p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Chuck");
    p.setProperty("mapObject", relatives);

    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(), p.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    p = database.save(p);
    database.commit();

    rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);
    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    loaded.<Map<String, Object>>getProperty("mapObject").put("brother", "Nike");

    relatives.put("brother", "Nike");
    database.save(loaded);
    database.commit();

    database.begin();
    for (Entry<String, Object> entry : relatives.entrySet()) {
      loaded = database.bindToSession(loaded);
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }
    database.commit();
    database.close();
    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);

    Assert.assertNotNull(loaded.<Map<String, Object>>getProperty("mapObject"));
    for (Entry<String, Object> entry : relatives.entrySet()) {
      Assert.assertEquals(
          entry.getValue(),
          loaded.<Map<String, Object>>getProperty("mapObject").get(entry.getKey()));
    }

    database.delete(database.bindToSession(loaded));
    database.commit();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test(dependsOnMethods = "embeddedMapObjectTest")
  public void testNoGenericCollections() {
    var p = database.newInstance("JavaNoGenericCollectionsTestClass");
    Entity c1 = database.newInstance("Child");
    c1.setProperty("name", "1");
    Entity c2 = database.newInstance("Child");
    c2.setProperty("name", "2");
    Entity c3 = database.newInstance("Child");
    c3.setProperty("name", "3");
    Entity c4 = database.newInstance("Child");
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

    database.begin();
    p = database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();
    database.close();
    database = createSessionInstance();
    database.begin();
    p = database.load(rid);

    Assert.assertEquals(p.<List>getProperty("list").size(), 4);
    Assert.assertEquals(p.<Set>getProperty("set").size(), 4);
    Assert.assertEquals(p.<Map>getProperty("map").size(), 4);
    for (int i = 0; i < 4; i++) {
      Object o = p.<List>getProperty("list").get(i);
      Assert.assertTrue(o instanceof Entity);
      Assert.assertEquals(((Entity) o).getProperty("name"), (i + 1) + "");
      o = p.<Map>getProperty("map").get((i + 1) + "");
      Assert.assertTrue(o instanceof Entity);
      Assert.assertEquals(((Entity) o).getProperty("name"), (i + 1) + "");
    }
    for (Object o : p.<Set>getProperty("set")) {
      Assert.assertTrue(o instanceof Entity);
      int nameToInt = Integer.parseInt(((Entity) o).getProperty("name"));
      Assert.assertTrue(nameToInt > 0 && nameToInt < 5);
    }

    var other = database.newEntity("JavaSimpleTestClass");
    p.<List>getProperty("list").add(other);
    p.<Set>getProperty("set").add(other);
    p.<Map>getProperty("map").put("5", other);

    database.save(p);
    database.commit();

    database.close();
    database = createSessionInstance();
    database.begin();
    p = database.load(rid);
    Assert.assertEquals(p.<List>getProperty("list").size(), 5);
    Object o = p.<List>getProperty("list").get(4);
    Assert.assertTrue(o instanceof Entity);
    o = p.<Map>getProperty("map").get("5");
    Assert.assertTrue(o instanceof Entity);
    boolean hasOther = false;
    for (Object obj : p.<Set>getProperty("set")) {
      hasOther = hasOther || (obj instanceof Entity);
    }
    Assert.assertTrue(hasOther);
    database.commit();
  }

  public void oidentifableFieldsTest() {
    Entity p = database.newInstance("JavaComplexTestClass");
    p.setProperty("name", "Dean Winchester");

    EntityImpl testEmbeddedDocument = new EntityImpl();
    testEmbeddedDocument.field("testEmbeddedField", "testEmbeddedValue");

    p.setProperty("embeddedDocument", testEmbeddedDocument);

    EntityImpl testDocument = new EntityImpl();
    testDocument.field("testField", "testValue");

    database.begin();
    testDocument.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    testDocument = database.bindToSession(testDocument);
    p.setProperty("document", testDocument);

    Blob testRecordBytes =
        new RecordBytes(
            "this is a bytearray test. if you read this Object database has stored it correctly"
                .getBytes());

    p.setProperty("byteArray", testRecordBytes);

    database.save(p);
    database.commit();

    YTRID rid = p.getIdentity();

    database.close();

    database = createSessionInstance();
    database.begin();
    Entity loaded = database.load(rid);

    Assert.assertNotNull(loaded.getBlobProperty("byteArray"));
    try {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
      OLogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    Assert.assertTrue(loaded.getElementProperty("document") instanceof EntityImpl);
    Assert.assertEquals(
        loaded.getElementProperty("document").getProperty("testField"), "testValue");
    Assert.assertTrue(loaded.getElementProperty("document").getIdentity().isPersistent());

    Assert.assertTrue(loaded.getElementProperty("embeddedDocument") instanceof EntityImpl);
    Assert.assertEquals(
        loaded.getElementProperty("embeddedDocument").getProperty("testEmbeddedField"),
        "testEmbeddedValue");
    Assert.assertFalse(loaded.getElementProperty("embeddedDocument").getIdentity().isValid());

    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    p = database.newInstance("JavaComplexTestClass");
    byte[] thumbnailImageBytes =
        "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
            .getBytes();
    Blob oRecordBytes = new RecordBytes(thumbnailImageBytes);

    oRecordBytes.save();
    p.setProperty("byteArray", oRecordBytes);

    p = database.save(p);
    database.commit();

    database.begin();
    p = database.bindToSession(p);
    Assert.assertNotNull(p.getBlobProperty("byteArray"));
    try {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
      OLogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    rid = p.getIdentity();

    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);

    Assert.assertNotNull(loaded.getBlobProperty("byteArray"));
    try {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
      OLogManager.instance().error(this, "Error reading byte[]", ioe);
      throw new RuntimeException(ioe);
    }
    database.commit();
    database.close();
    database = createSessionInstance();

    database.begin();
    p = database.newInstance("JavaComplexTestClass");
    thumbnailImageBytes =
        "this is a bytearray test. if you read this Object database has stored it correctlyVERSION2"
            .getBytes();

    oRecordBytes = new RecordBytes(thumbnailImageBytes);
    oRecordBytes.save();
    p.setProperty("byteArray", oRecordBytes);

    p = database.save(p);
    database.commit();

    database.begin();
    p = database.bindToSession(p);
    Assert.assertNotNull(p.getBlobProperty("byteArray"));
    try {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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

    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    loaded = database.load(rid);

    loaded.getBlobProperty("byteArray");
    try {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
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
      OLogManager.instance().error(this, "Error reading byte[]", ioe);
    }
    database.commit();
  }

  @Test
  public void testObjectDelete() {
    Entity media = database.newEntity("Media");
    Blob testRecord = database.newBlob("This is a test".getBytes());

    media.setProperty("content", testRecord);

    database.begin();
    media = database.save(media);
    database.commit();

    database.begin();
    media = database.bindToSession(media);
    Assert.assertEquals(new String(media.getBlobProperty("content").toStream()), "This is a test");

    // try to delete
    database.delete(database.bindToSession(media));
    database.commit();
  }

  @Test(dependsOnMethods = "mapEnumAndInternalObjects")
  public void update() {
    int[] i = new int[]{0};

    database.executeInTxBatches((Iterator<EntityImpl>) database.browseClass("Account"),
        (session, a) -> {
          if (i[0] % 2 == 0) {
            var addresses = a.<List<YTIdentifiable>>getProperty("addresses");
            var newAddress = database.newEntity("Address");

            newAddress.setProperty("street", "Plaza central");
            newAddress.setProperty("type", "work");

            var city = database.newEntity("City");
            city.setProperty("name", "Madrid");

            var country = database.newEntity("Country");
            country.setProperty("name", "Spain");

            city.setProperty("country", country);
            newAddress.setProperty("city", city);

            var newAddresses = new ArrayList<>(addresses);
            newAddresses.add(0, newAddress);
            a.setProperty("addresses", newAddresses);
          }

          a.setProperty("salary", (i[0] + 500.10f));

          session.save(a);
          i[0]++;
        });
  }

  @Test(dependsOnMethods = "update")
  public void testUpdate() {
    int i = 0;
    database.begin();
    Entity a;
    for (var iterator = database.query("select from Account"); iterator.hasNext(); ) {
      a = iterator.next().toEntity();

      if (i % 2 == 0) {
        Assert.assertEquals(
            a.<List<YTIdentifiable>>getProperty("addresses")
                .get(0)
                .<Entity>getRecord()
                .<YTIdentifiable>getProperty("city")
                .<Entity>getRecord()
                .<Entity>getRecord()
                .<YTIdentifiable>getProperty("country")
                .<Entity>getRecord()
                .getProperty("name"),
            "Spain");
      } else {
        Assert.assertEquals(
            a.<List<YTIdentifiable>>getProperty("addresses")
                .get(0)
                .<Entity>getRecord()
                .<YTIdentifiable>getProperty("city")
                .<Entity>getRecord()
                .<Entity>getRecord()
                .<YTIdentifiable>getProperty("country")
                .<Entity>getRecord()
                .getProperty("name"),
            "Italy");
      }

      Assert.assertEquals(a.<Float>getProperty("salary"), i + 500.1f);

      i++;
    }
    database.commit();
  }

  @Test(dependsOnMethods = "testUpdate")
  public void checkLazyLoadingOff() {
    long profiles = database.countClass("Profile");

    database.begin();
    Entity neo = database.newEntity("Profile");
    neo.setProperty("nick", "Neo");
    neo.setProperty("value", 1);

    var address = database.newEntity("Address");
    address.setProperty("street", "Rio de Castilla");
    address.setProperty("type", "residence");

    var city = database.newEntity("City");
    city.setProperty("name", "Madrid");

    var country = database.newEntity("Country");
    country.setProperty("name", "Spain");

    city.setProperty("country", country);
    address.setProperty("city", city);

    var morpheus = database.newEntity("Profile");
    morpheus.setProperty("nick", "Morpheus");

    var trinity = database.newEntity("Profile");
    trinity.setProperty("nick", "Trinity");

    var followers = new HashSet<>();
    followers.add(trinity);
    followers.add(morpheus);

    neo.setProperty("followers", followers);
    neo.setProperty("location", address);

    database.save(neo);
    database.commit();

    database.begin();
    Assert.assertEquals(database.countClass("Profile"), profiles + 3);

    for (Entity obj : database.browseClass("Profile")) {
      var followersList = obj.<Set<YTIdentifiable>>getProperty("followers");
      Assert.assertTrue(followersList == null || followersList instanceof LinkSet);
      if (obj.<String>getProperty("nick").equals("Neo")) {
        Assert.assertEquals(obj.<Set<YTIdentifiable>>getProperty("followers").size(), 2);
        Assert.assertEquals(
            obj.<Set<YTIdentifiable>>getProperty("followers")
                .iterator()
                .next()
                .getEntity()
                .getClassName(),
            "Profile");
      } else if (obj.<String>getProperty("nick").equals("Morpheus")
          || obj.<String>getProperty("nick").equals("Trinity")) {
        Assert.assertNull(obj.<Set<YTIdentifiable>>getProperty("followers"));
      }
    }
    database.commit();
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryPerFloat() {
    database.begin();
    final List<EntityImpl> result = executeQuery("select * from Account where salary = 500.10");

    Assert.assertFalse(result.isEmpty());

    Entity account;
    for (EntityImpl entries : result) {
      account = entries;
      Assert.assertEquals(account.<Float>getProperty("salary"), 500.10f);
    }
    database.commit();
  }

  @Test(dependsOnMethods = "checkLazyLoadingOff")
  public void queryCross3Levels() {
    database.begin();
    final List<EntityImpl> result =
        executeQuery("select from Profile where location.city.country.name = 'Spain'");

    Assert.assertFalse(result.isEmpty());

    Entity profile;
    for (EntityImpl entries : result) {
      profile = entries;
      Assert.assertEquals(
          profile
              .getElementProperty("location")
              .<Entity>getRecord()
              .<YTIdentifiable>getProperty("city")
              .<Entity>getRecord()
              .<Entity>getRecord()
              .<YTIdentifiable>getProperty("country")
              .<Entity>getRecord()
              .getProperty("name"),
          "Spain");
    }
    database.commit();
  }

  @Test(dependsOnMethods = "queryCross3Levels")
  public void deleteFirst() {
    startRecordNumber = database.countClass("Account");

    // DELETE ALL THE RECORD IN THE CLASS
    database.forEachInTx((Iterator<EntityImpl>) database.browseClass("Account"),
        ((session, document) -> {
          session.delete(document);
          return false;
        }));

    Assert.assertEquals(database.countClass("Account"), startRecordNumber - 1);
  }

  @Test
  public void commandWithPositionalParameters() {
    database.begin();
    List<EntityImpl> result =
        executeQuery("select from Profile where name = ? and surname = ?", "Barack", "Obama");

    Assert.assertFalse(result.isEmpty());
    database.commit();
  }

  @Test
  public void queryWithPositionalParameters() {
    database.begin();
    List<EntityImpl> result =
        executeQuery("select from Profile where name = ? and surname = ?", "Barack", "Obama");

    Assert.assertFalse(result.isEmpty());
    database.commit();
  }

  @Test
  public void queryWithRidAsParameters() {
    database.begin();
    Entity profile = database.browseClass("Profile").next();
    List<EntityImpl> result =
        executeQuery("select from Profile where @rid = ?", profile.getIdentity());

    Assert.assertEquals(result.size(), 1);
    database.commit();
  }

  @Test
  public void queryWithRidStringAsParameters() {
    database.begin();
    Entity profile = database.browseClass("Profile").next();
    List<EntityImpl> result =
        executeQuery("select from Profile where @rid = ?", profile.getIdentity());

    Assert.assertEquals(result.size(), 1);
    database.commit();
  }

  @Test
  public void commandWithNamedParameters() {
    addBarackObamaAndFollowers();

    HashMap<String, String> params = new HashMap<>();
    params.put("name", "Barack");
    params.put("surname", "Obama");

    database.begin();
    List<EntityImpl> result =
        executeQuery("select from Profile where name = :name and surname = :surname", params);
    Assert.assertFalse(result.isEmpty());
    database.commit();
  }

  @Test
  public void commandWithWrongNamedParameters() {
    try {
      HashMap<String, String> params = new HashMap<>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      executeQuery("select from Profile where name = :name and surname = :surname%", params);
      Assert.fail();
    } catch (YTCommandSQLParsingException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void queryConcatAttrib() {
    database.begin();
    Assert.assertFalse(executeQuery("select from City where country.@class = 'Country'").isEmpty());
    Assert.assertEquals(
        executeQuery("select from City where country.@class = 'Country22'").size(), 0);
    database.commit();
  }

  @Test
  public void queryPreparedTwice() {
    try (var db = acquireSession()) {
      db.begin();

      HashMap<String, String> params = new HashMap<>();
      params.put("name", "Barack");
      params.put("surname", "Obama");

      List<Entity> result =
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
    database.begin();
    var parent = database.newInstance("Parent");
    parent.setProperty("name", "Big Parent");

    var embedded = database.newInstance("EmbeddedChild");
    embedded.setProperty("name", "Little Child");

    parent.setProperty("embeddedChild", embedded);

    parent = database.save(parent);

    List<EntityImpl> presult = executeQuery("select from Parent");
    List<EntityImpl> cresult = executeQuery("select from EmbeddedChild");
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 0);

    var child = database.newInstance("EmbeddedChild");
    child.setProperty("name", "Little Child");
    parent.setProperty("child", child);

    parent = database.save(parent);
    database.commit();

    database.begin();
    presult = executeQuery("select from Parent");
    cresult = executeQuery("select from EmbeddedChild");
    Assert.assertEquals(presult.size(), 1);
    Assert.assertEquals(cresult.size(), 0);

    database.delete(database.bindToSession(parent));
    database.commit();

    database.begin();
    presult = executeQuery("select * from Parent");
    cresult = executeQuery("select * from EmbeddedChild");

    Assert.assertEquals(presult.size(), 0);
    Assert.assertEquals(cresult.size(), 0);
    database.commit();
  }

  @Test(enabled = false, dependsOnMethods = "testCreate")
  public void testEmbeddedBinary() {
    Entity a = database.newEntity("Account");
    a.setProperty("name", "Chris");
    a.setProperty("surname", "Martin");
    a.setProperty("id", 0);
    a.setProperty("thumbnail", new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    a = database.save(a);
    database.commit();

    database.close();

    database = createSessionInstance();
    Entity aa = database.load(a.getIdentity());
    Assert.assertNotNull(a.getProperty("thumbnail"));
    Assert.assertNotNull(aa.getProperty("thumbnail"));
    byte[] b = aa.getProperty("thumbnail");
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(b[i], i);
    }
  }

  @Test
  public void queryById() {
    database.begin();
    List<EntityImpl> result1 = executeQuery("select from Profile limit 1");

    List<EntityImpl> result2 =
        executeQuery("select from Profile where @rid = ?", result1.get(0).getIdentity());

    Assert.assertFalse(result2.isEmpty());
    database.commit();
  }

  @Test
  public void queryByIdNewApi() {
    database.begin();
    database.command("insert into Profile set nick = 'foo', name='foo'").close();
    database.commit();

    database.begin();
    List<EntityImpl> result1 = executeQuery("select from Profile where nick = 'foo'");

    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(result1.get(0).getClassName(), "Profile");
    Entity profile = result1.get(0);

    Assert.assertEquals(profile.getProperty("nick"), "foo");
    database.commit();
  }

  @Test(dependsOnMethods = "testUpdate")
  public void testSaveMultiCircular() {
    database = createSessionInstance();
    try {
      startRecordNumber = database.countClusterElements("Profile");
      database.begin();
      var bObama = database.newInstance("Profile");
      bObama.setProperty("nick", "TheUSPresident");
      bObama.setProperty("name", "Barack");
      bObama.setProperty("surname", "Obama");

      var address = database.newInstance("Address");
      address.setProperty("type", "Residence");

      var city = database.newInstance("City");
      city.setProperty("name", "Washington");

      var country = database.newInstance("Country");
      country.setProperty("name", "USA");

      city.setProperty("country", country);
      address.setProperty("city", city);

      bObama.setProperty("location", address);

      var presidentSon1 = database.newInstance("Profile");
      presidentSon1.setProperty("nick", "PresidentSon10");
      presidentSon1.setProperty("name", "Malia Ann");
      presidentSon1.setProperty("surname", "Obama");
      presidentSon1.setProperty("invitedBy", bObama);

      var presidentSon2 = database.newInstance("Profile");
      presidentSon2.setProperty("nick", "PresidentSon20");
      presidentSon2.setProperty("name", "Natasha");
      presidentSon2.setProperty("surname", "Obama");
      presidentSon2.setProperty("invitedBy", bObama);

      var followers = new ArrayList<>();
      followers.add(presidentSon1);
      followers.add(presidentSon2);

      bObama.setProperty("followers", followers);

      database.save(bObama);
      database.commit();
    } finally {
      database.close();
    }
  }

  private void createSimpleArrayTestClass() {
    if (database.getSchema().existsClass("JavaSimpleArrayTestClass")) {
      database.getSchema().dropClass("JavaSimpleSimpleArrayTestClass");
    }

    var cls = database.createClass("JavaSimpleArrayTestClass");
    cls.createProperty(database, "text", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "numberSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "longSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "doubleSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "floatSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "byteSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "flagSimple", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "dateField", YTType.EMBEDDEDLIST);
  }

  private void createBinaryTestClass() {
    if (database.getSchema().existsClass("JavaBinaryTestClass")) {
      database.getSchema().dropClass("JavaBinaryTestClass");
    }

    var cls = database.createClass("JavaBinaryTestClass");
    cls.createProperty(database, "binaryData", YTType.BINARY);
  }

  private void createPersonClass() {
    if (database.getClass("PersonTest") == null) {
      var cls = database.createClass("PersonTest");
      cls.createProperty(database, "firstname", YTType.STRING);
      cls.createProperty(database, "friends", YTType.LINKSET);
    }
  }

  private void createEventClass() {
    if (database.getClass("Event") == null) {
      var cls = database.createClass("Event");
      cls.createProperty(database, "name", YTType.STRING);
      cls.createProperty(database, "date", YTType.DATE);
    }
  }

  private void createAgendaClass() {
    if (database.getClass("Agenda") == null) {
      var cls = database.createClass("Agenda");
      cls.createProperty(database, "events", YTType.EMBEDDEDLIST);
    }
  }

  private void createNonGenericClass() {
    if (database.getClass("JavaNoGenericCollectionsTestClass") == null) {
      var cls = database.createClass("JavaNoGenericCollectionsTestClass");
      cls.createProperty(database, "list", YTType.EMBEDDEDLIST);
      cls.createProperty(database, "set", YTType.EMBEDDEDSET);
      cls.createProperty(database, "map", YTType.EMBEDDEDMAP);
    }
  }

  private void createMediaClass() {
    if (database.getClass("Media") == null) {
      var cls = database.createClass("Media");
      cls.createProperty(database, "content", YTType.LINK);
      cls.createProperty(database, "name", YTType.STRING);
    }
  }

  private void createParentChildClasses() {
    if (database.getSchema().existsClass("Parent")) {
      database.getSchema().dropClass("Parent");
    }
    if (database.getSchema().existsClass("EmbeddedChild")) {
      database.getSchema().dropClass("EmbeddedChild");
    }

    var parentCls = database.createClass("Parent");
    parentCls.createProperty(database, "name", YTType.STRING);
    parentCls.createProperty(database, "child", YTType.EMBEDDED,
        database.getClass("EmbeddedChild"));
    parentCls.createProperty(database, "embeddedChild", YTType.EMBEDDED,
        database.getClass("EmbeddedChild"));

    var childCls = database.createClass("EmbeddedChild");
    childCls.createProperty(database, "name", YTType.STRING);
  }
}
