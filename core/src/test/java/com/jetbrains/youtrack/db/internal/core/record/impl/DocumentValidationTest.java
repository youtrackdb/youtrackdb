package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
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

public class DocumentValidationTest extends BaseMemoryInternalDatabase {

  @Test
  public void testRequiredValidation() {
    db.begin();
    EntityImpl doc = new EntityImpl();
    Identifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    SchemaClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "float", PropertyType.FLOAT).setMandatory(db, true);
    clazz.createProperty(db, "boolean", PropertyType.BOOLEAN).setMandatory(db, true);
    clazz.createProperty(db, "binary", PropertyType.BINARY).setMandatory(db, true);
    clazz.createProperty(db, "byte", PropertyType.BYTE).setMandatory(db, true);
    clazz.createProperty(db, "date", PropertyType.DATE).setMandatory(db, true);
    clazz.createProperty(db, "datetime", PropertyType.DATETIME).setMandatory(db, true);
    clazz.createProperty(db, "decimal", PropertyType.DECIMAL).setMandatory(db, true);
    clazz.createProperty(db, "double", PropertyType.DOUBLE).setMandatory(db, true);
    clazz.createProperty(db, "short", PropertyType.SHORT).setMandatory(db, true);
    clazz.createProperty(db, "string", PropertyType.STRING).setMandatory(db, true);
    clazz.createProperty(db, "link", PropertyType.LINK).setMandatory(db, true);
    clazz.createProperty(db, "embedded", PropertyType.EMBEDDED, embeddedClazz)
        .setMandatory(db, true);

    clazz.createProperty(db, "embeddedListNoClass", PropertyType.EMBEDDEDLIST)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedSetNoClass", PropertyType.EMBEDDEDSET).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMapNoClass", PropertyType.EMBEDDEDMAP).setMandatory(db, true);

    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setMandatory(db, true);
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setMandatory(db, true);
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setMandatory(db, true);

    EntityImpl d = new EntityImpl(clazz);
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

    EntityImpl embedded = new EntityImpl("EmbeddedValidation");
    embedded.field("int", 20);
    embedded.field("long", 20);
    d.field("embedded", embedded);

    EntityImpl embeddedInList = new EntityImpl("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    final ArrayList<EntityImpl> embeddedList = new ArrayList<EntityImpl>();
    embeddedList.add(embeddedInList);
    d.field("embeddedList", embeddedList);

    EntityImpl embeddedInSet = new EntityImpl("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    final Set<EntityImpl> embeddedSet = new HashSet<EntityImpl>();
    embeddedSet.add(embeddedInSet);
    d.field("embeddedSet", embeddedSet);

    EntityImpl embeddedInMap = new EntityImpl("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    final Map<String, EntityImpl> embeddedMap = new HashMap<String, EntityImpl>();
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
    SchemaClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embedded", PropertyType.EMBEDDED, embeddedClazz)
        .setMandatory(db, true);
    SchemaClass clazzNotVertex = db.getMetadata().getSchema().createClass("NotVertex");
    clazzNotVertex.createProperty(db, "embeddedSimple", PropertyType.EMBEDDED);

    EntityImpl d = new EntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    d.field("embedded", new EntityImpl("EmbeddedValidation").field("test", "test"));
    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.int"));
    }
    d = new EntityImpl(clazzNotVertex);
    checkField(d, "embeddedSimple", db.newVertex());
    db.begin();
    checkField(d, "embeddedSimple", db.newRegularEdge(db.newVertex(), db.newVertex()));
    db.commit();
  }

  @Test
  public void testValidationNotValidEmbeddedSet() {
    SchemaClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET, embeddedClazz)
        .setMandatory(db, true);

    EntityImpl d = new EntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Set<EntityImpl> embeddedSet = new HashSet<EntityImpl>();
    d.field("embeddedSet", embeddedSet);

    EntityImpl embeddedInSet = new EntityImpl("EmbeddedValidation");
    embeddedInSet.field("int", 30);
    embeddedInSet.field("long", 30);
    embeddedSet.add(embeddedInSet);

    EntityImpl embeddedInSet2 = new EntityImpl("EmbeddedValidation");
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
    SchemaClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST, embeddedClazz)
        .setMandatory(db, true);

    EntityImpl d = new EntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final ArrayList<EntityImpl> embeddedList = new ArrayList<EntityImpl>();
    d.field("embeddedList", embeddedList);

    EntityImpl embeddedInList = new EntityImpl("EmbeddedValidation");
    embeddedInList.field("int", 30);
    embeddedInList.field("long", 30);
    embeddedList.add(embeddedInList);

    EntityImpl embeddedInList2 = new EntityImpl("EmbeddedValidation");
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
    SchemaClass embeddedClazz = db.getMetadata().getSchema().createClass("EmbeddedValidation");
    embeddedClazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    embeddedClazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setMandatory(db, true);
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP, embeddedClazz)
        .setMandatory(db, true);

    EntityImpl d = new EntityImpl(clazz);
    d.field("int", 30);
    d.field("long", 30);
    final Map<String, EntityImpl> embeddedMap = new HashMap<String, EntityImpl>();
    d.field("embeddedMap", embeddedMap);

    EntityImpl embeddedInMap = new EntityImpl("EmbeddedValidation");
    embeddedInMap.field("int", 30);
    embeddedInMap.field("long", 30);
    embeddedMap.put("1", embeddedInMap);

    EntityImpl embeddedInMap2 = new EntityImpl("EmbeddedValidation");
    embeddedInMap2.field("int", 30);
    embeddedMap.put("2", embeddedInMap2);

    try {
      d.validate();
      fail("Validation doesn't throw exception");
    } catch (ValidationException e) {
      Assert.assertTrue(e.toString().contains("EmbeddedValidation.long"));
    }
  }

  private void checkRequireField(EntityImpl toCheck, String fieldName) {
    try {
      EntityImpl newD = toCheck.copy();
      newD.removeField(fieldName);
      newD.validate();
      fail();
    } catch (ValidationException v) {
    }
  }

  @Test
  public void testMaxValidation() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMax(db, "11");
    clazz.createProperty(db, "long", PropertyType.LONG).setMax(db, "11");
    clazz.createProperty(db, "float", PropertyType.FLOAT).setMax(db, "11");
    // clazz.createProperty("boolean", PropertyType.BOOLEAN) no meaning
    clazz.createProperty(db, "binary", PropertyType.BINARY).setMax(db, "11");
    clazz.createProperty(db, "byte", PropertyType.BYTE).setMax(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", PropertyType.DATE).setMax(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", PropertyType.DATETIME)
        .setMax(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", PropertyType.DECIMAL).setMax(db, "11");
    clazz.createProperty(db, "double", PropertyType.DOUBLE).setMax(db, "11");
    clazz.createProperty(db, "short", PropertyType.SHORT).setMax(db, "11");
    clazz.createProperty(db, "string", PropertyType.STRING).setMax(db, "11");
    // clazz.createProperty("link", PropertyType.LINK) no meaning
    // clazz.createProperty("embedded", PropertyType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST).setMax(db, "2");
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET).setMax(db, "2");
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP).setMax(db, "2");

    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setMax(db, "2");
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setMax(db, "2");
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setMax(db, "2");
    clazz.createProperty(db, "linkBag", PropertyType.LINKBAG).setMax(db, "2");

    EntityImpl d = new EntityImpl(clazz);
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
    d.field("linkList", Arrays.asList(new RecordId(40, 30), new RecordId(40, 34)));
    d.field(
        "linkSet",
        new HashSet<RecordId>(Arrays.asList(new RecordId(40, 30), new RecordId(40, 31))));
    HashMap<String, RecordId> cont1 = new HashMap<String, RecordId>();
    cont1.put("one", new RecordId(30, 30));
    cont1.put("two", new RecordId(30, 30));
    d.field("linkMap", cont1);
    RidBag bag1 = new RidBag(db);
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
    checkField(d, "embeddedSet", new HashSet<String>(Arrays.asList("a", "b", "d")));
    HashMap<String, String> con1 = new HashMap<String, String>();
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
        new HashSet<RecordId>(
            Arrays.asList(new RecordId(40, 30), new RecordId(40, 33), new RecordId(40, 31))));

    HashMap<String, RecordId> cont3 = new HashMap<String, RecordId>();
    cont3.put("one", new RecordId(30, 30));
    cont3.put("two", new RecordId(30, 30));
    cont3.put("three", new RecordId(30, 30));
    checkField(d, "linkMap", cont3);

    RidBag bag2 = new RidBag(db);
    bag2.add(new RecordId(40, 30));
    bag2.add(new RecordId(40, 33));
    bag2.add(new RecordId(40, 31));
    checkField(d, "linkBag", bag2);
  }

  @Test
  public void testMinValidation() {
    db.begin();
    EntityImpl doc = new EntityImpl();
    Identifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMin(db, "11");
    clazz.createProperty(db, "long", PropertyType.LONG).setMin(db, "11");
    clazz.createProperty(db, "float", PropertyType.FLOAT).setMin(db, "11");
    // clazz.createProperty("boolean", PropertyType.BOOLEAN) //no meaning
    clazz.createProperty(db, "binary", PropertyType.BINARY).setMin(db, "11");
    clazz.createProperty(db, "byte", PropertyType.BYTE).setMin(db, "11");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, cal.get(Calendar.HOUR) == 11 ? 0 : 1);
    SimpleDateFormat format = db.getStorage().getConfiguration().getDateFormatInstance();
    clazz.createProperty(db, "date", PropertyType.DATE).setMin(db, format.format(cal.getTime()));
    cal = Calendar.getInstance();
    cal.add(Calendar.HOUR, 1);
    format = db.getStorage().getConfiguration().getDateTimeFormatInstance();
    clazz.createProperty(db, "datetime", PropertyType.DATETIME)
        .setMin(db, format.format(cal.getTime()));

    clazz.createProperty(db, "decimal", PropertyType.DECIMAL).setMin(db, "11");
    clazz.createProperty(db, "double", PropertyType.DOUBLE).setMin(db, "11");
    clazz.createProperty(db, "short", PropertyType.SHORT).setMin(db, "11");
    clazz.createProperty(db, "string", PropertyType.STRING).setMin(db, "11");
    // clazz.createProperty("link", PropertyType.LINK) no meaning
    // clazz.createProperty("embedded", PropertyType.EMBEDDED) no meaning

    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST).setMin(db, "1");
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET).setMin(db, "1");
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP).setMin(db, "1");

    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setMin(db, "1");
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setMin(db, "1");
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setMin(db, "1");
    clazz.createProperty(db, "linkBag", PropertyType.LINKBAG).setMin(db, "1");

    EntityImpl d = new EntityImpl(clazz);
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
    // d.field("embedded", new EntityImpl().field("test", "test"));
    d.field("embeddedList", List.of("a"));
    d.field("embeddedSet", new HashSet<String>(List.of("a")));
    Map<String, String> map = new HashMap<String, String>();
    map.put("some", "value");
    d.field("embeddedMap", map);
    d.field("linkList", List.of(new RecordId(40, 50)));
    d.field("linkSet", new HashSet<RecordId>(List.of(new RecordId(40, 50))));
    HashMap<String, RecordId> map1 = new HashMap<String, RecordId>();
    map1.put("some", new RecordId(40, 50));
    d.field("linkMap", map1);
    RidBag bag1 = new RidBag(db);
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
    checkField(d, "linkBag", new RidBag(db));
  }

  @Test
  public void testNotNullValidation() {
    db.begin();
    EntityImpl doc = new EntityImpl();
    Identifiable id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();
    db.commit();

    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setNotNull(db, true);
    clazz.createProperty(db, "long", PropertyType.LONG).setNotNull(db, true);
    clazz.createProperty(db, "float", PropertyType.FLOAT).setNotNull(db, true);
    clazz.createProperty(db, "boolean", PropertyType.BOOLEAN).setNotNull(db, true);
    clazz.createProperty(db, "binary", PropertyType.BINARY).setNotNull(db, true);
    clazz.createProperty(db, "byte", PropertyType.BYTE).setNotNull(db, true);
    clazz.createProperty(db, "date", PropertyType.DATE).setNotNull(db, true);
    clazz.createProperty(db, "datetime", PropertyType.DATETIME).setNotNull(db, true);
    clazz.createProperty(db, "decimal", PropertyType.DECIMAL).setNotNull(db, true);
    clazz.createProperty(db, "double", PropertyType.DOUBLE).setNotNull(db, true);
    clazz.createProperty(db, "short", PropertyType.SHORT).setNotNull(db, true);
    clazz.createProperty(db, "string", PropertyType.STRING).setNotNull(db, true);
    clazz.createProperty(db, "link", PropertyType.LINK).setNotNull(db, true);
    clazz.createProperty(db, "embedded", PropertyType.EMBEDDED).setNotNull(db, true);

    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST).setNotNull(db, true);
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET).setNotNull(db, true);
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP).setNotNull(db, true);

    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setNotNull(db, true);
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setNotNull(db, true);
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setNotNull(db, true);

    EntityImpl d = new EntityImpl(clazz);
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
    d.field("embedded", new EntityImpl().field("test", "test"));
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
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "string", PropertyType.STRING).setRegexp(db, "[^Z]*");

    EntityImpl d = new EntityImpl(clazz);
    d.field("string", "yeah");
    d.validate();

    checkField(d, "string", "yaZah");
  }

  @Test
  public void testLinkedTypeValidation() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST)
        .setLinkedType(db, PropertyType.INTEGER);
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET)
        .setLinkedType(db, PropertyType.INTEGER);
    clazz.createProperty(db, "embeddedMap", PropertyType.EMBEDDEDMAP)
        .setLinkedType(db, PropertyType.INTEGER);

    EntityImpl d = new EntityImpl(clazz);
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
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    SchemaClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "link", PropertyType.LINK).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embedded", PropertyType.EMBEDDED).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", PropertyType.LINKBAG).setLinkedClass(db, clazz1);
    EntityImpl d = new EntityImpl(clazz);
    d.field("link", new EntityImpl(clazz1));
    d.field("embedded", new EntityImpl(clazz1));
    List<EntityImpl> list = List.of(new EntityImpl(clazz1));
    d.field("linkList", list);
    Set<EntityImpl> set = new HashSet<EntityImpl>(list);
    d.field("linkSet", set);
    List<EntityImpl> embeddedList = Arrays.asList(new EntityImpl(clazz1), null);
    d.field("embeddedList", embeddedList);
    Set<EntityImpl> embeddedSet = new HashSet<EntityImpl>(embeddedList);
    d.field("embeddedSet", embeddedSet);

    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    map.put("a", new EntityImpl(clazz1));
    d.field("linkMap", map);

    d.validate();

    checkField(d, "link", new EntityImpl(clazz));
    checkField(d, "embedded", new EntityImpl(clazz));

    checkField(d, "linkList", Arrays.asList("a", "b"));
    checkField(d, "linkSet", new HashSet<String>(Arrays.asList("a", "b")));

    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("a", "a1");
    map1.put("b", "a2");
    checkField(d, "linkMap", map1);

    checkField(d, "linkList", List.of(new EntityImpl(clazz)));
    checkField(d, "linkSet", new HashSet<EntityImpl>(List.of(new EntityImpl(clazz))));
    checkField(d, "embeddedList", List.of(new EntityImpl(clazz)));
    checkField(d, "embeddedSet", List.of(new EntityImpl(clazz)));
    RidBag bag = new RidBag(db);
    bag.add(new EntityImpl(clazz));
    checkField(d, "linkBag", bag);
    Map<String, EntityImpl> map2 = new HashMap<String, EntityImpl>();
    map2.put("a", new EntityImpl(clazz));
    checkField(d, "linkMap", map2);
  }

  @Test
  public void testValidLinkCollectionsUpdate() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Validation");
    SchemaClass clazz1 = db.getMetadata().getSchema().createClass("Validation1");
    clazz.createProperty(db, "linkList", PropertyType.LINKLIST).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkSet", PropertyType.LINKSET).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkMap", PropertyType.LINKMAP).setLinkedClass(db, clazz1);
    clazz.createProperty(db, "linkBag", PropertyType.LINKBAG).setLinkedClass(db, clazz1);
    EntityImpl d = new EntityImpl(clazz);
    d.field("link", new EntityImpl(clazz1));
    d.field("embedded", new EntityImpl(clazz1));
    List<EntityImpl> list = List.of(new EntityImpl(clazz1));
    d.field("linkList", list);
    Set<EntityImpl> set = new HashSet<EntityImpl>(list);
    d.field("linkSet", set);
    d.field("linkBag", new RidBag(db));

    Map<String, EntityImpl> map = new HashMap<String, EntityImpl>();
    map.put("a", new EntityImpl(clazz1));

    db.begin();
    d.field("linkMap", map);
    db.save(d);
    db.commit();

    try {
      db.begin();
      d = db.bindToSession(d);
      EntityImpl newD = d.copy();
      ((Collection) newD.field("linkList")).add(new EntityImpl(clazz));
      newD.validate();
      fail();
    } catch (ValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      EntityImpl newD = d.copy();
      ((Collection) newD.field("linkSet")).add(new EntityImpl(clazz));
      newD.validate();
      fail();
    } catch (ValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      EntityImpl newD = d.copy();
      ((RidBag) newD.field("linkBag")).add(new EntityImpl(clazz));
      newD.validate();
      fail();
    } catch (ValidationException v) {
      db.rollback();
    }

    try {
      db.begin();
      d = db.bindToSession(d);
      EntityImpl newD = d.copy();
      ((Map<String, EntityImpl>) newD.field("linkMap")).put("a", new EntityImpl(clazz));
      newD.validate();
      fail();
    } catch (ValidationException v) {
      db.rollback();
    }
  }

  private void checkField(EntityImpl toCheck, String field, Object newValue) {
    try {
      EntityImpl newD = toCheck.copy();
      newD.field(field, newValue);
      newD.validate();
      fail();
    } catch (ValidationException v) {
    }
  }
}
