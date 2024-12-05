package com.orientechnologies.core.record.impl;

import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTValidationException;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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
    YTEntityImpl doc = new YTEntityImpl();
    YTIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    YTClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "float", YTType.FLOAT).setMandatory(db, true);
    clazz.createProperty(db, "boolean", YTType.BOOLEAN).setMandatory(db, true);
    clazz.createProperty(db, "binary", YTType.BINARY).setMandatory(db, true);
    clazz.createProperty(db, "byte", YTType.BYTE).setMandatory(db, true);
    clazz.createProperty(db, "date", YTType.DATE).setMandatory(db, true);
    clazz.createProperty(db, "datetime", YTType.DATETIME).setMandatory(db, true);
    clazz.createProperty(db, "decimal", YTType.DECIMAL).setMandatory(db, true);
    clazz.createProperty(db, "double", YTType.DOUBLE).setMandatory(db, true);
    clazz.createProperty(db, "short", YTType.SHORT).setMandatory(db, true);
    clazz.createProperty(db, "string", YTType.STRING).setMandatory(db, true);
    clazz.createProperty(db, "link", YTType.LINK).setMandatory(db, true);
    clazz.createProperty(db, "embedded", YTType.EMBEDDED, embeddedClazz).setMandatory(db, true);

    clazz.createProperty(db, "embeddedListNoClass", YTType.EMBEDDEDLIST).setMandatory(db, true);
    clazz.createProperty(db, "embeddedSetNoClass", YTType.EMBEDDEDSET).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMapNoClass", YTType.EMBEDDEDMAP).setMandatory(db, true);

    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    clazz.createProperty(db, "linkList", YTType.LINKLIST).setMandatory(db, true);
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setMandatory(db, true);
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setMandatory(db, true);

    YTEntityImpl d = new YTEntityImpl(clazz);
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
    d.field("linkList", new ArrayList<YTRecordId>());
    d.field("linkSet", new HashSet<YTRecordId>());
    d.field("linkMap", new HashMap<String, YTRecordId>());

    d.field("embeddedListNoClass", new ArrayList<YTRecordId>());
    d.field("embeddedSetNoClass", new HashSet<YTRecordId>());
    d.field("embeddedMapNoClass", new HashMap<String, YTRecordId>());

    YTEntityImpl embedded = new YTEntityImpl("EmbeddedValidation");
    embedded.field("int", 20);
    embedded.field("long", 20);
    d.field("embedded", embedded);

    YTEntityImpl embeddedInList = new YTEntityImpl("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    final ArrayList<YTEntityImpl> embeddedList = new ArrayList<YTEntityImpl>();
    embeddedList.add(embeddedInList);
    d.field("embeddedList", embeddedList);

    YTEntityImpl embeddedInSet = new YTEntityImpl("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    final Set<YTEntityImpl> embeddedSet = new HashSet<YTEntityImpl>();
    embeddedSet.add(embeddedInSet);
    d.field("embeddedSet", embeddedSet);

    YTEntityImpl embeddedInMap = new YTEntityImpl("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    final Map<String, YTEntityImpl> embeddedMap = new HashMap<String, YTEntityImpl>();
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
    YTClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embedded", YTType.EMBEDDED, embeddedClazz).setMandatory(db, true);
    YTClass clazzNotVertex = db.getMetadata().getSchema().createClass("NotVertex");
    clazzNotVertex.createProperty(db, "embeddedSimple", YTType.EMBEDDED);

    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    d.field("embedded", new YTEntityImpl("EmbeddedValidation").field("test", "test"));
    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (YTValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.int"));
    }
    d = new YTEntityImpl(clazzNotVertex);
    checkField(d, "embeddedSimple", db.newVertex());
    db.begin();
    checkField(d, "embeddedSimple", db.newEdge(db.newVertex(), db.newVertex()));
    db.commit();
  }

  @Test
  public void testValidationNotValidEmbeddedSet() {
    YTClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);

    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Set<YTEntityImpl> embeddedSet = new HashSet<YTEntityImpl>();
    d.field("embeddedSet", embeddedSet);

    YTEntityImpl embeddedInSet = new YTEntityImpl("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    embeddedSet.add(embeddedInSet);

    YTEntityImpl embeddedInSet2 = new YTEntityImpl("EmbeddedValidation");
    embeddedInSet2.field("int", 30);
    embeddedSet.add(embeddedInSet2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (YTValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedList() {
    YTClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);

    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final ArrayList<YTEntityImpl> embeddedList = new ArrayList<YTEntityImpl>();
    d.field("embeddedList", embeddedList);

    YTEntityImpl embeddedInList = new YTEntityImpl("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    embeddedList.add(embeddedInList);

    YTEntityImpl embeddedInList2 = new YTEntityImpl("EmbeddedValidation");
    embeddedInList2.field("int", 30);
    embeddedList.add(embeddedInList2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (YTValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  @Test
  public void testValidationNotValidEmbeddedMap() {
    YTClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Map<String, YTEntityImpl> embeddedMap = new HashMap<String, YTEntityImpl>();
    d.field("embeddedMap", embeddedMap);

    YTEntityImpl embeddedInMap = new YTEntityImpl("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    embeddedMap.put("1", embeddedInMap);

    YTEntityImpl embeddedInMap2 = new YTEntityImpl("EmbeddedValidation");
    embeddedInMap2.field("int", 30);
    embeddedMap.put("2", embeddedInMap2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (YTValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  private void checkRequireField(YTEntityImpl toCheck, String fieldName) {
    try {
      YTEntityImpl newD = toCheck.copy();
      newD.removeField(fieldName);
      newD.validate();
      fail();
    } catch (YTValidationException v) {
    }
  }

  @Test
  public void testMaxValidation() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMax(db, "11");
    clazz.createProperty(db, "long", YTType.LONG).setMax(db, "11");
    clazz.createProperty(db, "float", YTType.FLOAT).setMax(db, "11");
    // clazz.createProperty("boolean", YTType.BOOLEAN) no meaning
    clazz.createProperty(db, "binary", YTType.BINARY).setMax(db, "11");
    clazz.createProperty(db, "byte", YTType.BYTE).setMax(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", YTType.DATE).setMax(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", YTType.DATETIME).setMax(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", YTType.DECIMAL).setMax(db, "11");
    clazz.createProperty(db, "double", YTType.DOUBLE).setMax(db, "11");
    clazz.createProperty(db, "short", YTType.SHORT).setMax(db, "11");
    clazz.createProperty(db, "string", YTType.STRING).setMax(db, "11");
    // clazz.createProperty("link", YTType.LINK) no meaning
    // clazz.createProperty("embedded", YTType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST).setMax(db, "2");
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET).setMax(db, "2");
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP).setMax(db, "2");

    clazz.createProperty(db, "linkList", YTType.LINKLIST).setMax(db, "2");
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setMax(db, "2");
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setMax(db, "2");
    clazz.createProperty(db, "linkBag", YTType.LINKBAG).setMax(db, "2");

    YTEntityImpl d = new YTEntityImpl(clazz);
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
    d.field("linkList", Arrays.asList(new YTRecordId(40, 30), new YTRecordId(40, 34)));
    d.field(
        "linkSet",
        new HashSet<YTRecordId>(Arrays.asList(new YTRecordId(40, 30), new YTRecordId(40, 31))));
    HashMap<String, YTRecordId> cont1 = new HashMap<String, YTRecordId>();
    cont1.put("one", new YTRecordId(30, 30));
    cont1.put("two", new YTRecordId(30, 30));
    d.field("linkMap", cont1);
    RidBag bag1 = new RidBag(db);
    bag1.add(new YTRecordId(40, 30));
    bag1.add(new YTRecordId(40, 33));
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
        Arrays.asList(new YTRecordId(40, 30), new YTRecordId(40, 33), new YTRecordId(40, 31)));
    checkField(
        d,
        "linkSet",
        new HashSet<YTRecordId>(
            Arrays.asList(new YTRecordId(40, 30), new YTRecordId(40, 33), new YTRecordId(40, 31))));

    HashMap<String, YTRecordId> cont3 = new HashMap<String, YTRecordId>();
    cont3.put("one", new YTRecordId(30, 30));
    cont3.put("two", new YTRecordId(30, 30));
    cont3.put("three", new YTRecordId(30, 30));
    checkField(d, "linkMap", cont3);

    RidBag bag2 = new RidBag(db);
    bag2.add(new YTRecordId(40, 30));
    bag2.add(new YTRecordId(40, 33));
    bag2.add(new YTRecordId(40, 31));
    checkField(d, "linkBag", bag2);
  }

  @Test
  public void testMinValidation() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl();
    YTIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setMin(db, "11");
    clazz.createProperty(db, "long", YTType.LONG).setMin(db, "11");
    clazz.createProperty(db, "float", YTType.FLOAT).setMin(db, "11");
    // clazz.createProperty("boolean", YTType.BOOLEAN) //no meaning
    clazz.createProperty(db, "binary", YTType.BINARY).setMin(db, "11");
    clazz.createProperty(db, "byte", YTType.BYTE).setMin(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", YTType.DATE).setMin(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", YTType.DATETIME).setMin(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", YTType.DECIMAL).setMin(db, "11");
    clazz.createProperty(db, "double", YTType.DOUBLE).setMin(db, "11");
    clazz.createProperty(db, "short", YTType.SHORT).setMin(db, "11");
    clazz.createProperty(db, "string", YTType.STRING).setMin(db, "11");
    // clazz.createProperty("link", YTType.LINK) no meaning
    // clazz.createProperty("embedded", YTType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST).setMin(db, "1");
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET).setMin(db, "1");
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP).setMin(db, "1");

    clazz.createProperty(db, "linkList", YTType.LINKLIST).setMin(db, "1");
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setMin(db, "1");
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setMin(db, "1");
    clazz.createProperty(db, "linkBag", YTType.LINKBAG).setMin(db, "1");

    YTEntityImpl d = new YTEntityImpl(clazz);
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
    // d.field("embedded", new YTEntityImpl().field("test", "test"));
    d.field("embeddedList", List.of("a"));
    d.field("embeddedSet", new HashSet<String>(List.of("a")));
    Map<String, String> map = new HashMap<String, String>();
    map.put("some", "value");
    d.field("embeddedMap", map);
    d.field("linkList", List.of(new YTRecordId(40, 50)));
    d.field("linkSet", new HashSet<YTRecordId>(List.of(new YTRecordId(40, 50))));
    HashMap<String, YTRecordId> map1 = new HashMap<String, YTRecordId>();
    map1.put("some", new YTRecordId(40, 50));
    d.field("linkMap", map1);
    RidBag bag1 = new RidBag(db);
    bag1.add(new YTRecordId(40, 50));
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
    checkField(d, "linkList", new ArrayList<YTRecordId>());
    checkField(d, "linkSet", new HashSet<YTRecordId>());
    checkField(d, "linkMap", new HashMap<String, YTRecordId>());
    checkField(d, "linkBag", new RidBag(db));
  }

  @Test
  public void testNotNullValidation() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl();
    YTIdentifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", YTType.INTEGER).setNotNull(db, true);
    clazz.createProperty(db, "long", YTType.LONG).setNotNull(db, true);
    clazz.createProperty(db, "float", YTType.FLOAT).setNotNull(db, true);
    clazz.createProperty(db, "boolean", YTType.BOOLEAN).setNotNull(db, true);
    clazz.createProperty(db, "binary", YTType.BINARY).setNotNull(db, true);
    clazz.createProperty(db, "byte", YTType.BYTE).setNotNull(db, true);
    clazz.createProperty(db, "date", YTType.DATE).setNotNull(db, true);
    clazz.createProperty(db, "datetime", YTType.DATETIME).setNotNull(db, true);
    clazz.createProperty(db, "decimal", YTType.DECIMAL).setNotNull(db, true);
    clazz.createProperty(db, "double", YTType.DOUBLE).setNotNull(db, true);
    clazz.createProperty(db, "short", YTType.SHORT).setNotNull(db, true);
    clazz.createProperty(db, "string", YTType.STRING).setNotNull(db, true);
    clazz.createProperty(db, "link", YTType.LINK).setNotNull(db, true);
    clazz.createProperty(db, "embedded", YTType.EMBEDDED).setNotNull(db, true);

    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST).setNotNull(db, true);
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET).setNotNull(db, true);
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP).setNotNull(db, true);

    clazz.createProperty(db, "linkList", YTType.LINKLIST).setNotNull(db, true);
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setNotNull(db, true);
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setNotNull(db, true);

    YTEntityImpl d = new YTEntityImpl(clazz);
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
    d.field("embedded", new YTEntityImpl().field("test", "test"));
    d.field("embeddedList", new ArrayList<String>());
    d.field("embeddedSet", new HashSet<String>());
    d.field("embeddedMap", new HashMap<String, String>());
    d.field("linkList", new ArrayList<YTRecordId>());
    d.field("linkSet", new HashSet<YTRecordId>());
    d.field("linkMap", new HashMap<String, YTRecordId>());
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
    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "string", YTType.STRING).setRegexp(db, "[^Z]*");

    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("string", "yeah");
    d.validate();

    checkField(d, "string", "yaZah");
  }

  @Test
  public void testLinkedTypeValidation() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST).setLinkedType(db, YTType.INTEGER);
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET).setLinkedType(db, YTType.INTEGER);
    clazz.createProperty(db, "embeddedMap", YTType.EMBEDDEDMAP).setLinkedType(db, YTType.INTEGER);

    YTEntityImpl d = new YTEntityImpl(clazz);
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
    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    YTClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "link", YTType.LINK).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embedded", YTType.EMBEDDED).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkList", YTType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedList", YTType.EMBEDDEDLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedSet", YTType.EMBEDDEDSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", YTType.LINKBAG).setLinkedClass(db, clazz1);
    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("link", new YTEntityImpl(clazz1));
    d.field("embedded", new YTEntityImpl(clazz1));
    List<YTEntityImpl> list = List.of(new YTEntityImpl(clazz1));
    d.field("linkList", list);
    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>(list);
    d.field("linkSet", set);
    List<YTEntityImpl> embeddedList = Arrays.asList(new YTEntityImpl(clazz1), null);
    d.field("embeddedList", embeddedList);
    Set<YTEntityImpl> embeddedSet = new HashSet<YTEntityImpl>(embeddedList);
    d.field("embeddedSet", embeddedSet);

    Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    map.put("a", new YTEntityImpl(clazz1));
    d.field("linkMap", map);

    d.validate();

    checkField(d, "link", new YTEntityImpl(clazz));
    checkField(d, "embedded", new YTEntityImpl(clazz));

    checkField(d, "linkList", Arrays.asList("a", "b"));
    checkField(d, "linkSet", new HashSet<String>(Arrays.asList("a", "b")));

    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "linkMap", map1);

    checkField(d, "linkList", List.of(new YTEntityImpl(clazz)));
    checkField(d, "linkSet", new HashSet<YTEntityImpl>(List.of(new YTEntityImpl(clazz))));
    checkField(d, "embeddedList", List.of(new YTEntityImpl(clazz)));
    checkField(d, "embeddedSet", List.of(new YTEntityImpl(clazz)));
    RidBag bag = new RidBag(db);
    bag.add(new YTEntityImpl(clazz));
    checkField(d, "linkBag", bag);
    Map<String, YTEntityImpl> map2 = new HashMap<String, YTEntityImpl>();
    map2.put("a", new YTEntityImpl(clazz));
    checkField(d, "linkMap", map2);
  }

  @Test
  public void testValidLinkCollectionsUpdate() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Validation");
    YTClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "linkList", YTType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", YTType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", YTType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", YTType.LINKBAG).setLinkedClass(db, clazz1);
    YTEntityImpl d = new YTEntityImpl(clazz);
    d.field("link", new YTEntityImpl(clazz1));
    d.field("embedded", new YTEntityImpl(clazz1));
    List<YTEntityImpl> list = List.of(new YTEntityImpl(clazz1));
    d.field("linkList", list);
    Set<YTEntityImpl> set = new HashSet<YTEntityImpl>(list);
    d.field("linkSet", set);
    d.field("linkBag", new RidBag(db));

    Map<String, YTEntityImpl> map = new HashMap<String, YTEntityImpl>();
    map.put("a", new YTEntityImpl(clazz1));

    db.begin();
    d.field("linkMap", map);
    db.save(d);
    db.commit();

    try {
      db.begin();
      d = db.bindToSession(d);
      YTEntityImpl newD = d.copy();
      ((Collection) newD.field("linkList")).add(new YTEntityImpl(clazz));
      newD.validate();
      fail();
    } catch (YTValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      YTEntityImpl newD = d.copy();
      ((Collection) newD.field("linkSet")).add(new YTEntityImpl(clazz));
      newD.validate();
      fail();
    } catch (YTValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      YTEntityImpl newD = d.copy();
      ((RidBag) newD.field("linkBag")).add(new YTEntityImpl(clazz));
      newD.validate();
      fail();
    } catch (YTValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      YTEntityImpl newD = d.copy();
      ((Map<String, YTEntityImpl>) newD.field("linkMap")).put("a", new YTEntityImpl(clazz));
      newD.validate();
      fail();
    } catch (YTValidationException v) {
      db.rollback();
    }
  }

  private void checkField(YTEntityImpl toCheck, String field, Object newValue) {
    try {
      YTEntityImpl newD = toCheck.copy();
      newD.field(field, newValue);
      newD.validate();
      fail();
    } catch (YTValidationException v) {
    }
  }
}
