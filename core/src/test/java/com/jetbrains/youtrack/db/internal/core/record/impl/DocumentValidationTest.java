package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class DocumentValidationTest extends BaseMemoryInternalDatabase {

  @Test
  public void testRequiredValidation() {
    session.begin();
    var doc = (EntityImpl) session.newEntity();
    Identifiable id = session.save(doc).getIdentity();
    session.commit();

    var embeddedClazz = session.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);
    clazz.createProperty(session, "float", PropertyType.FLOAT).setMandatory(session, true);
    clazz.createProperty(session, "boolean", PropertyType.BOOLEAN).setMandatory(session, true);
    clazz.createProperty(session, "binary", PropertyType.BINARY).setMandatory(session, true);
    clazz.createProperty(session, "byte", PropertyType.BYTE).setMandatory(session, true);
    clazz.createProperty(session, "date", PropertyType.DATE).setMandatory(session, true);
    clazz.createProperty(session, "datetime", PropertyType.DATETIME).setMandatory(session, true);
    clazz.createProperty(session, "decimal", PropertyType.DECIMAL).setMandatory(session, true);
    clazz.createProperty(session, "double", PropertyType.DOUBLE).setMandatory(session, true);
    clazz.createProperty(session, "short", PropertyType.SHORT).setMandatory(session, true);
    clazz.createProperty(session, "string", PropertyType.STRING).setMandatory(session, true);
    clazz.createProperty(session, "link", PropertyType.LINK).setMandatory(session, true);
    clazz.createProperty(session, "embedded", PropertyType.EMBEDDED, embeddedClazz)
        .setMandatory(session, true);

    clazz.createProperty(session, "embeddedListNoClass", PropertyType.EMBEDDEDLIST)
        .setMandatory(session, true);
    clazz.createProperty(session, "embeddedSetNoClass", PropertyType.EMBEDDEDSET).setMandatory(
        session, true);
    clazz.createProperty(session, "embeddedMapNoClass", PropertyType.EMBEDDEDMAP).setMandatory(
        session, true);

    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(session, true);
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(session, true);
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(session, true);

    clazz.createProperty(session, "linkList", PropertyType.LINKLIST).setMandatory(session, true);
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setMandatory(session, true);
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setMandatory(session, true);

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 10);
    d.field("long", 10);
    d.field("float", 10);
    d.field("boolean", 10);
    d.field("binary", new byte[]{});
    d.field("byte", 10);
    d.field("date", new Date());
    d.field("datetime", new Date());
    d.field("decimal", 10);
    d.field("double", 10);
    d.field("short", 10);
    d.field("string", "yeah");
    d.field("link", id);
    d.field("linkList", new ArrayList<RecordId>());
    d.field("linkSet", new HashSet<RecordId>());
    d.field("linkMap", new HashMap<String, RecordId>());

    d.field("embeddedListNoClass", new ArrayList<RecordId>());
    d.field("embeddedSetNoClass", new HashSet<RecordId>());
    d.field("embeddedMapNoClass", new HashMap<String, RecordId>());

    var embedded = (EntityImpl) session.newEntity("EmbeddedValidation");
    embedded.field("int", 20);
    embedded.field("long", 20);
    d.field("embedded", embedded);

    var embeddedInList = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    final var embeddedList = new ArrayList<EntityImpl>();
    embeddedList.add(embeddedInList);
    d.field("embeddedList", embeddedList);

    var embeddedInSet = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    final Set<EntityImpl> embeddedSet = new HashSet<>();
    embeddedSet.add(embeddedInSet);
    d.field("embeddedSet", embeddedSet);

    var embeddedInMap = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    final Map<String, EntityImpl> embeddedMap = new HashMap<>();
    embeddedMap.put("testEmbedded", embeddedInMap);
    d.field("embeddedMap", embeddedMap);

    d.validate();

    checkRequireField(d, "int");
    checkRequireField(d, "long");
    checkRequireField(d, "float");
    checkRequireField(d, "boolean");
    checkRequireField(d, "binary");
    checkRequireField(d, "byte");
    checkRequireField(d, "date");
    checkRequireField(d, "datetime");
    checkRequireField(d, "decimal");
    checkRequireField(d, "double");
    checkRequireField(d, "short");
    checkRequireField(d, "string");
    checkRequireField(d, "link");
    checkRequireField(d, "embedded");
    checkRequireField(d, "embeddedList");
    checkRequireField(d, "embeddedSet");
    checkRequireField(d, "embeddedMap");
    checkRequireField(d, "linkList");
    checkRequireField(d, "linkSet");
    checkRequireField(d, "linkMap");
  }

  @Test
  public void testValidationNotValidEmbedded() {
    var embeddedClazz = session.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);
    clazz.createProperty(session, "embedded", PropertyType.EMBEDDED, embeddedClazz)
        .setMandatory(session, true);
    var clazzNotVertex = session.getMetadata().getSchema().createClass("NotVertex");
    clazzNotVertex.createProperty(session, "embeddedSimple", PropertyType.EMBEDDED);

    session.begin();
    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 30);
    d.field("long", 30);
    d.field("embedded",
        ((EntityImpl) session.newEntity("EmbeddedValidation")).field("test", "test"));
    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.int"));
      session.rollback();
    }

    session.begin();
    d = (EntityImpl) session.newEntity(clazzNotVertex);
    checkField(d, "embeddedSimple", session.newVertex());
    checkField(d, "embeddedSimple",
        session.newRegularEdge(session.newVertex(), session.newVertex()));
    session.rollback();
  }

  @Test
  public void testValidationNotValidEmbeddedSet() {
    var embeddedClazz = session.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    embeddedClazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(session, true);

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Set<EntityImpl> embeddedSet = new HashSet<>();
    d.field("embeddedSet", embeddedSet);

    var embeddedInSet = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    embeddedSet.add(embeddedInSet);

    var embeddedInSet2 = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInSet2.field("int", 30);
    embeddedSet.add(embeddedInSet2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedList() {
    var embeddedClazz = session.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    embeddedClazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);
    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(session, true);

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final var embeddedList = new ArrayList<EntityImpl>();
    d.field("embeddedList", embeddedList);

    var embeddedInList = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    embeddedList.add(embeddedInList);

    var embeddedInList2 = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInList2.field("int", 30);
    embeddedList.add(embeddedInList2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedMap() {
    var embeddedClazz = session.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    embeddedClazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMandatory(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setMandatory(session, true);
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(session, true);

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Map<String, EntityImpl> embeddedMap = new HashMap<>();
    d.field("embeddedMap", embeddedMap);

    var embeddedInMap = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    embeddedMap.put("1", embeddedInMap);

    var embeddedInMap2 = (EntityImpl) session.newEntity("EmbeddedValidation");
    embeddedInMap2.field("int", 30);
    embeddedMap.put("2", embeddedInMap2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  private static void checkRequireField(EntityImpl toCheck, String fieldName) {
    try {
      var session = toCheck.getSession();
      var newD = (EntityImpl) session.newEntity(toCheck.getSchemaClass());
      newD.copyPropertiesFromOtherEntity(toCheck);
      newD.removeField(fieldName);
      newD.validate();
      fail();
    } catch (ValidationException v) {
    }
  }

  @Test
  public void testMaxValidation() {
    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMax(session, "11");
    clazz.createProperty(session, "long", PropertyType.LONG).setMax(session, "11");
    clazz.createProperty(session, "float", PropertyType.FLOAT).setMax(session, "11");
    // clazz.createProperty("boolean", PropertyType.BOOLEAN) no meaning
    clazz.createProperty(session, "binary", PropertyType.BINARY).setMax(session, "11");
    clazz.createProperty(session, "byte", PropertyType.BYTE).setMax(session, "11");
    var cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    var format = session.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(session, "date", PropertyType.DATE)
        .setMax(session, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = session.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(session, "datetime", PropertyType.DATETIME)
        .setMax(session, format.format(cal.getTime()));

    clazz.createProperty(session, "decimal", PropertyType.DECIMAL).setMax(session, "11");
    clazz.createProperty(session, "double", PropertyType.DOUBLE).setMax(session, "11");
    clazz.createProperty(session, "short", PropertyType.SHORT).setMax(session, "11");
    clazz.createProperty(session, "string", PropertyType.STRING).setMax(session, "11");
    // clazz.createProperty("link", PropertyType.LINK) no meaning
    // clazz.createProperty("embedded", PropertyType.EMBEDDED) no meaning

    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST).setMax(session, "2");
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET).setMax(session, "2");
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP).setMax(session, "2");

    clazz.createProperty(session, "linkList", PropertyType.LINKLIST).setMax(session, "2");
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setMax(session, "2");
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setMax(session, "2");
    clazz.createProperty(session, "linkBag", PropertyType.LINKBAG).setMax(session, "2");

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 11);
    d.field("long", 11);
    d.field("float", 11);
    d.field("binary", new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
    d.field("byte", 11);
    d.field("date", new Date());
    d.field("datetime", new Date());
    d.field("decimal", 10);
    d.field("double", 10);
    d.field("short", 10);
    d.field("string", "yeah");
    d.field("embeddedList", Arrays.asList("a", "b"));
    d.field("embeddedSet", new HashSet<>(Arrays.asList("a", "b")));
    var cont = new HashMap<String, String>();
    cont.put("one", "one");
    cont.put("two", "one");
    d.field("embeddedMap", cont);
    d.field("linkList", Arrays.asList(new RecordId(40, 30), new RecordId(40, 34)));
    d.field(
        "linkSet",
        new HashSet<>(Arrays.asList(new RecordId(40, 30), new RecordId(40, 31))));
    var cont1 = new HashMap<String, RecordId>();
    cont1.put("one", new RecordId(30, 30));
    cont1.put("two", new RecordId(30, 30));
    d.field("linkMap", cont1);
    var bag1 = new RidBag(session);
    bag1.add(new RecordId(40, 30));
    bag1.add(new RecordId(40, 33));
    d.field("linkBag", bag1);
    d.validate();

    checkField(d, "int", 12);
    checkField(d, "long", 12);
    checkField(d, "float", 20);
    checkField(d, "binary", new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});

    checkField(d, "byte", 20);
    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 1);
    checkField(d, "date", cal.getTime());
    checkField(d, "datetime", cal.getTime());
    checkField(d, "decimal", 20);
    checkField(d, "double", 20);
    checkField(d, "short", 20);
    checkField(d, "string", "0123456789101112");
    checkField(d, "embeddedList", Arrays.asList("a", "b", "d"));
    checkField(d, "embeddedSet", new HashSet<>(Arrays.asList("a", "b", "d")));
    var con1 = new HashMap<String, String>();
    con1.put("one", "one");
    con1.put("two", "one");
    con1.put("three", "one");

    checkField(d, "embeddedMap", con1);
    checkField(
        d,
        "linkList",
        Arrays.asList(new RecordId(40, 30), new RecordId(40, 33), new RecordId(40, 31)));
    checkField(
        d,
        "linkSet",
        new HashSet<>(
            Arrays.asList(new RecordId(40, 30), new RecordId(40, 33), new RecordId(40, 31))));

    var cont3 = new HashMap<String, RecordId>();
    cont3.put("one", new RecordId(30, 30));
    cont3.put("two", new RecordId(30, 30));
    cont3.put("three", new RecordId(30, 30));
    checkField(d, "linkMap", cont3);

    var bag2 = new RidBag(session);
    bag2.add(new RecordId(40, 30));
    bag2.add(new RecordId(40, 33));
    bag2.add(new RecordId(40, 31));
    checkField(d, "linkBag", bag2);
  }

  @Test
  public void testMinValidation() {
    session.begin();
    var doc = (EntityImpl) session.newEntity();
    Identifiable id = session.save(doc).getIdentity();
    session.commit();

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setMin(session, "11");
    clazz.createProperty(session, "long", PropertyType.LONG).setMin(session, "11");
    clazz.createProperty(session, "float", PropertyType.FLOAT).setMin(session, "11");
    // clazz.createProperty("boolean", PropertyType.BOOLEAN) //no meaning
    clazz.createProperty(session, "binary", PropertyType.BINARY).setMin(session, "11");
    clazz.createProperty(session, "byte", PropertyType.BYTE).setMin(session, "11");
    var cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    var format = session.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(session, "date", PropertyType.DATE)
        .setMin(session, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = session.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(session, "datetime", PropertyType.DATETIME)
        .setMin(session, format.format(cal.getTime()));

    clazz.createProperty(session, "decimal", PropertyType.DECIMAL).setMin(session, "11");
    clazz.createProperty(session, "double", PropertyType.DOUBLE).setMin(session, "11");
    clazz.createProperty(session, "short", PropertyType.SHORT).setMin(session, "11");
    clazz.createProperty(session, "string", PropertyType.STRING).setMin(session, "11");
    // clazz.createProperty("link", PropertyType.LINK) no meaning
    // clazz.createProperty("embedded", PropertyType.EMBEDDED) no meaning

    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST).setMin(session, "1");
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET).setMin(session, "1");
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP).setMin(session, "1");

    clazz.createProperty(session, "linkList", PropertyType.LINKLIST).setMin(session, "1");
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setMin(session, "1");
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setMin(session, "1");
    clazz.createProperty(session, "linkBag", PropertyType.LINKBAG).setMin(session, "1");

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 11);
    d.field("long", 11);
    d.field("float", 11);
    d.field("binary", new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
    d.field("byte", 11);

    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, 1);
    d.field("date", new Date());
    d.field("datetime", cal.getTime());
    d.field("decimal", 12);
    d.field("double", 12);
    d.field("short", 12);
    d.field("string", "yeahyeahyeah");
    d.field("link", id);
    // d.field("embedded", (EntityImpl)db.newEntity().field("test", "test"));
    d.field("embeddedList", List.of("a"));
    d.field("embeddedSet", new HashSet<>(List.of("a")));
    Map<String, String> map = new HashMap<>();
    map.put("some", "value");
    d.field("embeddedMap", map);
    d.field("linkList", List.of(new RecordId(40, 50)));
    d.field("linkSet", new HashSet<>(List.of(new RecordId(40, 50))));
    var map1 = new HashMap<String, RecordId>();
    map1.put("some", new RecordId(40, 50));
    d.field("linkMap", map1);
    var bag1 = new RidBag(session);
    bag1.add(new RecordId(40, 50));
    d.field("linkBag", bag1);
    d.validate();

    checkField(d, "int", 10);
    checkField(d, "long", 10);
    checkField(d, "float", 10);
    checkField(d, "binary", new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
    checkField(d, "byte", 10);

    cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -1);
    checkField(d, "date", cal.getTime());
    checkField(d, "datetime", new Date());
    checkField(d, "decimal", 10);
    checkField(d, "double", 10);
    checkField(d, "short", 10);
    checkField(d, "string", "01234");
    checkField(d, "embeddedList", new ArrayList<String>());
    checkField(d, "embeddedSet", new HashSet<String>());
    checkField(d, "embeddedMap", new HashMap<String, String>());
    checkField(d, "linkList", new ArrayList<RecordId>());
    checkField(d, "linkSet", new HashSet<RecordId>());
    checkField(d, "linkMap", new HashMap<String, RecordId>());
    checkField(d, "linkBag", new RidBag(session));
  }

  @Test
  public void testNotNullValidation() {
    session.begin();
    var doc = (EntityImpl) session.newEntity();
    Identifiable id = session.save(doc).getIdentity();
    session.commit();

    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "int", PropertyType.INTEGER).setNotNull(session, true);
    clazz.createProperty(session, "long", PropertyType.LONG).setNotNull(session, true);
    clazz.createProperty(session, "float", PropertyType.FLOAT).setNotNull(session, true);
    clazz.createProperty(session, "boolean", PropertyType.BOOLEAN).setNotNull(session, true);
    clazz.createProperty(session, "binary", PropertyType.BINARY).setNotNull(session, true);
    clazz.createProperty(session, "byte", PropertyType.BYTE).setNotNull(session, true);
    clazz.createProperty(session, "date", PropertyType.DATE).setNotNull(session, true);
    clazz.createProperty(session, "datetime", PropertyType.DATETIME).setNotNull(session, true);
    clazz.createProperty(session, "decimal", PropertyType.DECIMAL).setNotNull(session, true);
    clazz.createProperty(session, "double", PropertyType.DOUBLE).setNotNull(session, true);
    clazz.createProperty(session, "short", PropertyType.SHORT).setNotNull(session, true);
    clazz.createProperty(session, "string", PropertyType.STRING).setNotNull(session, true);
    clazz.createProperty(session, "link", PropertyType.LINK).setNotNull(session, true);
    clazz.createProperty(session, "embedded", PropertyType.EMBEDDED).setNotNull(session, true);

    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST)
        .setNotNull(session, true);
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET)
        .setNotNull(session, true);
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP)
        .setNotNull(session, true);

    clazz.createProperty(session, "linkList", PropertyType.LINKLIST).setNotNull(session, true);
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setNotNull(session, true);
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setNotNull(session, true);

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("int", 12);
    d.field("long", 12);
    d.field("float", 12);
    d.field("boolean", true);
    d.field("binary", new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
    d.field("byte", 12);
    d.field("date", new Date());
    d.field("datetime", new Date());
    d.field("decimal", 12);
    d.field("double", 12);
    d.field("short", 12);
    d.field("string", "yeah");
    d.field("link", id);
    d.field("embedded", ((EntityImpl) session.newEntity()).field("test", "test"));
    d.field("embeddedList", new ArrayList<String>());
    d.field("embeddedSet", new HashSet<String>());
    d.field("embeddedMap", new HashMap<String, String>());
    d.field("linkList", new ArrayList<RecordId>());
    d.field("linkSet", new HashSet<RecordId>());
    d.field("linkMap", new HashMap<String, RecordId>());
    d.validate();

    checkField(d, "int", null);
    checkField(d, "long", null);
    checkField(d, "float", null);
    checkField(d, "boolean", null);
    checkField(d, "binary", null);
    checkField(d, "byte", null);
    checkField(d, "date", null);
    checkField(d, "datetime", null);
    checkField(d, "decimal", null);
    checkField(d, "double", null);
    checkField(d, "short", null);
    checkField(d, "string", null);
    checkField(d, "link", null);
    checkField(d, "embedded", null);
    checkField(d, "embeddedList", null);
    checkField(d, "embeddedSet", null);
    checkField(d, "embeddedMap", null);
    checkField(d, "linkList", null);
    checkField(d, "linkSet", null);
    checkField(d, "linkMap", null);
  }

  @Test
  public void testRegExpValidation() {
    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "string", PropertyType.STRING).setRegexp(session, "[^Z]*");

    var d = (EntityImpl) session.newEntity(clazz);
    d.field("string", "yeah");
    d.validate();

    checkField(d, "string", "yaZah");
  }

  @Test
  public void testLinkedTypeValidation() {
    var clazz = session.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST)
        .setLinkedType(session, PropertyType.INTEGER);
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET)
        .setLinkedType(session, PropertyType.INTEGER);
    clazz.createProperty(session, "embeddedMap", PropertyType.EMBEDDEDMAP)
        .setLinkedType(session, PropertyType.INTEGER);

    var d = (EntityImpl) session.newEntity(clazz);
    var list = Arrays.asList(1, 2);
    d.field("embeddedList", list);
    Set<Integer> set = new HashSet<>(list);
    d.field("embeddedSet", set);

    Map<String, Integer> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    d.field("embeddedMap", map);

    d.validate();

    checkField(d, "embeddedList", Arrays.asList("a", "b"));
    checkField(d, "embeddedSet", new HashSet<>(Arrays.asList("a", "b")));
    Map<String, String> map1 = new HashMap<>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "embeddedMap", map1);
  }

  @Test
  public void testLinkedClassValidation() {
    var clazz = session.getMetadata().getSchema().createClass("Validation");
    var clazz1 = session.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(session, "link", PropertyType.LINK).setLinkedClass(session, clazz1);
    clazz.createProperty(session, "embedded", PropertyType.EMBEDDED)
        .setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkList", PropertyType.LINKLIST)
        .setLinkedClass(session, clazz1);
    clazz.createProperty(session, "embeddedList", PropertyType.EMBEDDEDLIST)
        .setLinkedClass(session, clazz1);
    clazz.createProperty(session, "embeddedSet", PropertyType.EMBEDDEDSET)
        .setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkBag", PropertyType.LINKBAG).setLinkedClass(session, clazz1);
    var d = (EntityImpl) session.newEntity(clazz);
    d.field("link", session.newEntity(clazz1));
    d.field("embedded", session.newEntity(clazz1));
    var list = List.of((EntityImpl) session.newEntity(clazz1));
    d.field("linkList", list);
    Set<EntityImpl> set = new HashSet<>(list);
    d.field("linkSet", set);
    var embeddedList = Arrays.asList((EntityImpl) session.newEntity(clazz1), null);
    d.field("embeddedList", embeddedList);
    Set<EntityImpl> embeddedSet = new HashSet<>(embeddedList);
    d.field("embeddedSet", embeddedSet);

    Map<String, EntityImpl> map = new HashMap<>();
    map.put("a", (EntityImpl) session.newEntity(clazz1));
    d.field("linkMap", map);

    d.validate();

    checkField(d, "link", session.newEntity(clazz));
    checkField(d, "embedded", session.newEntity(clazz));

    checkField(d, "linkList", Arrays.asList("a", "b"));
    checkField(d, "linkSet", new HashSet<>(Arrays.asList("a", "b")));

    Map<String, String> map1 = new HashMap<>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "linkMap", map1);

    checkField(d, "linkList", List.of((EntityImpl) session.newEntity(clazz)));
    checkField(d, "linkSet", new HashSet<>(List.of((EntityImpl) session.newEntity(clazz))));
    checkField(d, "embeddedList", List.of((EntityImpl) session.newEntity(clazz)));
    checkField(d, "embeddedSet", List.of((EntityImpl) session.newEntity(clazz)));
    var bag = new RidBag(session);
    bag.add(session.newEntity(clazz).getIdentity());
    checkField(d, "linkBag", bag);
    Map<String, EntityImpl> map2 = new HashMap<>();
    map2.put("a", (EntityImpl) session.newEntity(clazz));
    checkField(d, "linkMap", map2);
  }

  @Test
  public void testValidLinkCollectionsUpdate() {
    var clazz = session.getMetadata().getSchema().createClass("Validation");
    var clazz1 = session.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(session, "linkList", PropertyType.LINKLIST)
        .setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkSet", PropertyType.LINKSET).setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkMap", PropertyType.LINKMAP).setLinkedClass(session, clazz1);
    clazz.createProperty(session, "linkBag", PropertyType.LINKBAG).setLinkedClass(session, clazz1);
    var d = (EntityImpl) session.newEntity(clazz);
    d.field("link", session.newEntity(clazz1));
    d.field("embedded", session.newEntity(clazz1));
    var list = List.of((EntityImpl) session.newEntity(clazz1));
    d.field("linkList", list);
    Set<EntityImpl> set = new HashSet<>(list);
    d.field("linkSet", set);
    d.field("linkBag", new RidBag(session));

    Map<String, EntityImpl> map = new HashMap<>();
    map.put("a", (EntityImpl) session.newEntity(clazz1));

    session.begin();
    d.field("linkMap", map);
    session.save(d);
    session.commit();

    try {
      session.begin();
      d = session.bindToSession(d);
      ((Collection) d.field("linkList")).add(session.newEntity(clazz));
      d.validate();
      fail();
    } catch (ValidationException v) {
      session.rollback();
    }

    try {
      session.begin();
      d = session.bindToSession(d);
      ((Collection) d.field("linkSet")).add(session.newEntity(clazz));
      d.validate();
      fail();
    } catch (ValidationException v) {
      session.rollback();
    }

    try {
      session.begin();
      d = session.bindToSession(d);
      ((RidBag) d.field("linkBag")).add(session.newEntity(clazz).getIdentity());
      session.commit();
      fail();
    } catch (ValidationException v) {
      session.rollback();
    }

    try {
      session.begin();
      d = session.bindToSession(d);
      ((Map<String, EntityImpl>) d.field("linkMap")).put("a",
          (EntityImpl) session.newEntity(clazz));
      d.validate();
      fail();
    } catch (ValidationException v) {
      session.rollback();
    }
  }

  private void checkField(EntityImpl toCheck, String field, Object newValue) {
    try {
      var session = toCheck.getSession();
      var newD = (EntityImpl) session.newEntity(toCheck.getSchemaClass());
      newD.copyPropertiesFromOtherEntity(toCheck);
      newD.field(field, newValue);
      newD.validate();
      fail();
    } catch (ValidationException v) {
    }
  }
}
