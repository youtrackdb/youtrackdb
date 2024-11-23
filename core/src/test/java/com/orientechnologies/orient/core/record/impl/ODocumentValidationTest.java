package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.text.SimpleDateFormat;
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

public class ODocumentValidationTest extends BaseMemoryInternalDatabase {

  @Test
  public void testRequiredValidation() {
    db.begin();
    ODocument doc = new ODocument();
    OIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    OClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "float", OType.FLOAT).setMandatory(db, true);
    clazz.createProperty(db, "boolean", OType.BOOLEAN).setMandatory(db, true);
    clazz.createProperty(db, "binary", OType.BINARY).setMandatory(db, true);
    clazz.createProperty(db, "byte", OType.BYTE).setMandatory(db, true);
    clazz.createProperty(db, "date", OType.DATE).setMandatory(db, true);
    clazz.createProperty(db, "datetime", OType.DATETIME).setMandatory(db, true);
    clazz.createProperty(db, "decimal", OType.DECIMAL).setMandatory(db, true);
    clazz.createProperty(db, "double", OType.DOUBLE).setMandatory(db, true);
    clazz.createProperty(db, "short", OType.SHORT).setMandatory(db, true);
    clazz.createProperty(db, "string", OType.STRING).setMandatory(db, true);
    clazz.createProperty(db, "link", OType.LINK).setMandatory(db, true);
    clazz.createProperty(db, "embedded", OType.EMBEDDED, embeddedClazz).setMandatory(db, true);

    clazz.createProperty(db, "embeddedListNoClass", OType.EMBEDDEDLIST).setMandatory(db, true);
    clazz.createProperty(db, "embeddedSetNoClass", OType.EMBEDDEDSET).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMapNoClass", OType.EMBEDDEDMAP).setMandatory(db, true);

    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    clazz.createProperty(db, "linkList", OType.LINKLIST).setMandatory(db, true);
    clazz.createProperty(db, "linkSet", OType.LINKSET).setMandatory(db, true);
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setMandatory(db, true);

    ODocument d = new ODocument(clazz);
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
    d.field("linkList", new ArrayList<ORecordId>());
    d.field("linkSet", new HashSet<ORecordId>());
    d.field("linkMap", new HashMap<String, ORecordId>());

    d.field("embeddedListNoClass", new ArrayList<ORecordId>());
    d.field("embeddedSetNoClass", new HashSet<ORecordId>());
    d.field("embeddedMapNoClass", new HashMap<String, ORecordId>());

    ODocument embedded = new ODocument("EmbeddedValidation");
    embedded.field("int", 20);
    embedded.field("long", 20);
    d.field("embedded", embedded);

    ODocument embeddedInList = new ODocument("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    final ArrayList<ODocument> embeddedList = new ArrayList<ODocument>();
    embeddedList.add(embeddedInList);
    d.field("embeddedList", embeddedList);

    ODocument embeddedInSet = new ODocument("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    final Set<ODocument> embeddedSet = new HashSet<ODocument>();
    embeddedSet.add(embeddedInSet);
    d.field("embeddedSet", embeddedSet);

    ODocument embeddedInMap = new ODocument("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    final Map<String, ODocument> embeddedMap = new HashMap<String, ODocument>();
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
    OClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embedded", OType.EMBEDDED, embeddedClazz).setMandatory(db, true);
    OClass clazzNotVertex = db.getMetadata().getSchema().createClass("NotVertex");
    clazzNotVertex.createProperty(db, "embeddedSimple", OType.EMBEDDED);

    ODocument d = new ODocument(clazz);
    d.field("int", 30);
    d.field("long", 30);
    d.field("embedded", new ODocument("EmbeddedValidation").field("test", "test"));
    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (OValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.int"));
    }
    d = new ODocument(clazzNotVertex);
    checkField(d, "embeddedSimple", db.newVertex());
    db.begin();
    checkField(d, "embeddedSimple", db.newEdge(db.newVertex(), db.newVertex()));
    db.commit();
  }

  @Test
  public void testValidationNotValidEmbeddedSet() {
    OClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);

    ODocument d = new ODocument(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Set<ODocument> embeddedSet = new HashSet<ODocument>();
    d.field("embeddedSet", embeddedSet);

    ODocument embeddedInSet = new ODocument("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    embeddedSet.add(embeddedInSet);

    ODocument embeddedInSet2 = new ODocument("EmbeddedValidation");
    embeddedInSet2.field("int", 30);
    embeddedSet.add(embeddedInSet2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (OValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedList() {
    OClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);

    ODocument d = new ODocument(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final ArrayList<ODocument> embeddedList = new ArrayList<ODocument>();
    d.field("embeddedList", embeddedList);

    ODocument embeddedInList = new ODocument("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    embeddedList.add(embeddedInList);

    ODocument embeddedInList2 = new ODocument("EmbeddedValidation");
    embeddedInList2.field("int", 30);
    embeddedList.add(embeddedInList2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (OValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedMap() {
    OClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", OType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    ODocument d = new ODocument(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Map<String, ODocument> embeddedMap = new HashMap<String, ODocument>();
    d.field("embeddedMap", embeddedMap);

    ODocument embeddedInMap = new ODocument("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    embeddedMap.put("1", embeddedInMap);

    ODocument embeddedInMap2 = new ODocument("EmbeddedValidation");
    embeddedInMap2.field("int", 30);
    embeddedMap.put("2", embeddedInMap2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (OValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  private void checkRequireField(ODocument toCheck, String fieldName) {
    try {
      ODocument newD = toCheck.copy();
      newD.removeField(fieldName);
      newD.validate();
      fail();
    } catch (OValidationException v) {
    }
  }

  @Test
  public void testMaxValidation() {
    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMax(db, "11");
    clazz.createProperty(db, "long", OType.LONG).setMax(db, "11");
    clazz.createProperty(db, "float", OType.FLOAT).setMax(db, "11");
    // clazz.createProperty("boolean", OType.BOOLEAN) no meaning
    clazz.createProperty(db, "binary", OType.BINARY).setMax(db, "11");
    clazz.createProperty(db, "byte", OType.BYTE).setMax(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", OType.DATE).setMax(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", OType.DATETIME).setMax(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", OType.DECIMAL).setMax(db, "11");
    clazz.createProperty(db, "double", OType.DOUBLE).setMax(db, "11");
    clazz.createProperty(db, "short", OType.SHORT).setMax(db, "11");
    clazz.createProperty(db, "string", OType.STRING).setMax(db, "11");
    // clazz.createProperty("link", OType.LINK) no meaning
    // clazz.createProperty("embedded", OType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST).setMax(db, "2");
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET).setMax(db, "2");
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP).setMax(db, "2");

    clazz.createProperty(db, "linkList", OType.LINKLIST).setMax(db, "2");
    clazz.createProperty(db, "linkSet", OType.LINKSET).setMax(db, "2");
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setMax(db, "2");
    clazz.createProperty(db, "linkBag", OType.LINKBAG).setMax(db, "2");

    ODocument d = new ODocument(clazz);
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
    d.field("embeddedSet", new HashSet<String>(Arrays.asList("a", "b")));
    HashMap<String, String> cont = new HashMap<String, String>();
    cont.put("one", "one");
    cont.put("two", "one");
    d.field("embeddedMap", cont);
    d.field("linkList", Arrays.asList(new ORecordId(40, 30), new ORecordId(40, 34)));
    d.field(
        "linkSet",
        new HashSet<ORecordId>(Arrays.asList(new ORecordId(40, 30), new ORecordId(40, 31))));
    HashMap<String, ORecordId> cont1 = new HashMap<String, ORecordId>();
    cont1.put("one", new ORecordId(30, 30));
    cont1.put("two", new ORecordId(30, 30));
    d.field("linkMap", cont1);
    ORidBag bag1 = new ORidBag();
    bag1.add(new ORecordId(40, 30));
    bag1.add(new ORecordId(40, 33));
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
    checkField(d, "embeddedSet", new HashSet<String>(Arrays.asList("a", "b", "d")));
    HashMap<String, String> con1 = new HashMap<String, String>();
    con1.put("one", "one");
    con1.put("two", "one");
    con1.put("three", "one");

    checkField(d, "embeddedMap", con1);
    checkField(
        d,
        "linkList",
        Arrays.asList(new ORecordId(40, 30), new ORecordId(40, 33), new ORecordId(40, 31)));
    checkField(
        d,
        "linkSet",
        new HashSet<ORecordId>(
            Arrays.asList(new ORecordId(40, 30), new ORecordId(40, 33), new ORecordId(40, 31))));

    HashMap<String, ORecordId> cont3 = new HashMap<String, ORecordId>();
    cont3.put("one", new ORecordId(30, 30));
    cont3.put("two", new ORecordId(30, 30));
    cont3.put("three", new ORecordId(30, 30));
    checkField(d, "linkMap", cont3);

    ORidBag bag2 = new ORidBag();
    bag2.add(new ORecordId(40, 30));
    bag2.add(new ORecordId(40, 33));
    bag2.add(new ORecordId(40, 31));
    checkField(d, "linkBag", bag2);
  }

  @Test
  public void testMinValidation() {
    db.begin();
    ODocument doc = new ODocument();
    OIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setMin(db, "11");
    clazz.createProperty(db, "long", OType.LONG).setMin(db, "11");
    clazz.createProperty(db, "float", OType.FLOAT).setMin(db, "11");
    // clazz.createProperty("boolean", OType.BOOLEAN) //no meaning
    clazz.createProperty(db, "binary", OType.BINARY).setMin(db, "11");
    clazz.createProperty(db, "byte", OType.BYTE).setMin(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", OType.DATE).setMin(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", OType.DATETIME).setMin(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", OType.DECIMAL).setMin(db, "11");
    clazz.createProperty(db, "double", OType.DOUBLE).setMin(db, "11");
    clazz.createProperty(db, "short", OType.SHORT).setMin(db, "11");
    clazz.createProperty(db, "string", OType.STRING).setMin(db, "11");
    // clazz.createProperty("link", OType.LINK) no meaning
    // clazz.createProperty("embedded", OType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST).setMin(db, "1");
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET).setMin(db, "1");
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP).setMin(db, "1");

    clazz.createProperty(db, "linkList", OType.LINKLIST).setMin(db, "1");
    clazz.createProperty(db, "linkSet", OType.LINKSET).setMin(db, "1");
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setMin(db, "1");
    clazz.createProperty(db, "linkBag", OType.LINKBAG).setMin(db, "1");

    ODocument d = new ODocument(clazz);
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
    // d.field("embedded", new ODocument().field("test", "test"));
    d.field("embeddedList", List.of("a"));
    d.field("embeddedSet", new HashSet<String>(List.of("a")));
    Map<String, String> map = new HashMap<String, String>();
    map.put("some", "value");
    d.field("embeddedMap", map);
    d.field("linkList", List.of(new ORecordId(40, 50)));
    d.field("linkSet", new HashSet<ORecordId>(List.of(new ORecordId(40, 50))));
    HashMap<String, ORecordId> map1 = new HashMap<String, ORecordId>();
    map1.put("some", new ORecordId(40, 50));
    d.field("linkMap", map1);
    ORidBag bag1 = new ORidBag();
    bag1.add(new ORecordId(40, 50));
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
    checkField(d, "linkList", new ArrayList<ORecordId>());
    checkField(d, "linkSet", new HashSet<ORecordId>());
    checkField(d, "linkMap", new HashMap<String, ORecordId>());
    checkField(d, "linkBag", new ORidBag());
  }

  @Test
  public void testNotNullValidation() {
    db.begin();
    ODocument doc = new ODocument();
    OIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", OType.INTEGER).setNotNull(db, true);
    clazz.createProperty(db, "long", OType.LONG).setNotNull(db, true);
    clazz.createProperty(db, "float", OType.FLOAT).setNotNull(db, true);
    clazz.createProperty(db, "boolean", OType.BOOLEAN).setNotNull(db, true);
    clazz.createProperty(db, "binary", OType.BINARY).setNotNull(db, true);
    clazz.createProperty(db, "byte", OType.BYTE).setNotNull(db, true);
    clazz.createProperty(db, "date", OType.DATE).setNotNull(db, true);
    clazz.createProperty(db, "datetime", OType.DATETIME).setNotNull(db, true);
    clazz.createProperty(db, "decimal", OType.DECIMAL).setNotNull(db, true);
    clazz.createProperty(db, "double", OType.DOUBLE).setNotNull(db, true);
    clazz.createProperty(db, "short", OType.SHORT).setNotNull(db, true);
    clazz.createProperty(db, "string", OType.STRING).setNotNull(db, true);
    clazz.createProperty(db, "link", OType.LINK).setNotNull(db, true);
    clazz.createProperty(db, "embedded", OType.EMBEDDED).setNotNull(db, true);

    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST).setNotNull(db, true);
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET).setNotNull(db, true);
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP).setNotNull(db, true);

    clazz.createProperty(db, "linkList", OType.LINKLIST).setNotNull(db, true);
    clazz.createProperty(db, "linkSet", OType.LINKSET).setNotNull(db, true);
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setNotNull(db, true);

    ODocument d = new ODocument(clazz);
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
    d.field("embedded", new ODocument().field("test", "test"));
    d.field("embeddedList", new ArrayList<String>());
    d.field("embeddedSet", new HashSet<String>());
    d.field("embeddedMap", new HashMap<String, String>());
    d.field("linkList", new ArrayList<ORecordId>());
    d.field("linkSet", new HashSet<ORecordId>());
    d.field("linkMap", new HashMap<String, ORecordId>());
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
    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "string", OType.STRING).setRegexp(db, "[^Z]*");

    ODocument d = new ODocument(clazz);
    d.field("string", "yeah");
    d.validate();

    checkField(d, "string", "yaZah");
  }

  @Test
  public void testLinkedTypeValidation() {
    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST).setLinkedType(db, OType.INTEGER);
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET).setLinkedType(db, OType.INTEGER);
    clazz.createProperty(db, "embeddedMap", OType.EMBEDDEDMAP).setLinkedType(db, OType.INTEGER);

    ODocument d = new ODocument(clazz);
    List<Integer> list = Arrays.asList(1, 2);
    d.field("embeddedList", list);
    Set<Integer> set = new HashSet<Integer>(list);
    d.field("embeddedSet", set);

    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("a", 1);
    map.put("b", 2);
    d.field("embeddedMap", map);

    d.validate();

    checkField(d, "embeddedList", Arrays.asList("a", "b"));
    checkField(d, "embeddedSet", new HashSet<String>(Arrays.asList("a", "b")));
    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "embeddedMap", map1);
  }

  @Test
  public void testLinkedClassValidation() {
    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    OClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "link", OType.LINK).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embedded", OType.EMBEDDED).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkList", OType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedList", OType.EMBEDDEDLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedSet", OType.EMBEDDEDSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", OType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", OType.LINKBAG).setLinkedClass(db, clazz1);
    ODocument d = new ODocument(clazz);
    d.field("link", new ODocument(clazz1));
    d.field("embedded", new ODocument(clazz1));
    List<ODocument> list = List.of(new ODocument(clazz1));
    d.field("linkList", list);
    Set<ODocument> set = new HashSet<ODocument>(list);
    d.field("linkSet", set);
    List<ODocument> embeddedList = Arrays.asList(new ODocument(clazz1), null);
    d.field("embeddedList", embeddedList);
    Set<ODocument> embeddedSet = new HashSet<ODocument>(embeddedList);
    d.field("embeddedSet", embeddedSet);

    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("a", new ODocument(clazz1));
    d.field("linkMap", map);

    d.validate();

    checkField(d, "link", new ODocument(clazz));
    checkField(d, "embedded", new ODocument(clazz));

    checkField(d, "linkList", Arrays.asList("a", "b"));
    checkField(d, "linkSet", new HashSet<String>(Arrays.asList("a", "b")));

    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "linkMap", map1);

    checkField(d, "linkList", List.of(new ODocument(clazz)));
    checkField(d, "linkSet", new HashSet<ODocument>(List.of(new ODocument(clazz))));
    checkField(d, "embeddedList", List.of(new ODocument(clazz)));
    checkField(d, "embeddedSet", List.of(new ODocument(clazz)));
    ORidBag bag = new ORidBag();
    bag.add(new ODocument(clazz));
    checkField(d, "linkBag", bag);
    Map<String, ODocument> map2 = new HashMap<String, ODocument>();
    map2.put("a", new ODocument(clazz));
    checkField(d, "linkMap", map2);
  }

  @Test
  public void testValidLinkCollectionsUpdate() {
    OClass clazz = db.getMetadata().getSchema().createClass("Validation");
    OClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "linkList", OType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", OType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", OType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", OType.LINKBAG).setLinkedClass(db, clazz1);
    ODocument d = new ODocument(clazz);
    d.field("link", new ODocument(clazz1));
    d.field("embedded", new ODocument(clazz1));
    List<ODocument> list = List.of(new ODocument(clazz1));
    d.field("linkList", list);
    Set<ODocument> set = new HashSet<ODocument>(list);
    d.field("linkSet", set);
    d.field("linkBag", new ORidBag());

    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("a", new ODocument(clazz1));

    db.begin();
    d.field("linkMap", map);
    db.save(d);
    db.commit();

    try {
      db.begin();
      d = db.bindToSession(d);
      ODocument newD = d.copy();
      ((Collection) newD.field("linkList")).add(new ODocument(clazz));
      newD.validate();
      fail();
    } catch (OValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      ODocument newD = d.copy();
      ((Collection) newD.field("linkSet")).add(new ODocument(clazz));
      newD.validate();
      fail();
    } catch (OValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      ODocument newD = d.copy();
      ((ORidBag) newD.field("linkBag")).add(new ODocument(clazz));
      newD.validate();
      fail();
    } catch (OValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      ODocument newD = d.copy();
      ((Map<String, ODocument>) newD.field("linkMap")).put("a", new ODocument(clazz));
      newD.validate();
      fail();
    } catch (OValidationException v) {
      db.rollback();
    }
  }

  private void checkField(ODocument toCheck, String field, Object newValue) {
    try {
      ODocument newD = toCheck.copy();
      newD.field(field, newValue);
      newD.validate();
      fail();
    } catch (OValidationException v) {
    }
  }
}
