package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
import java.util.Date;
import org.junit.Test;

public class DefaultValueTest extends DBTestBase {

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassC");

    YTProperty prop = classA.createProperty(db, "name", YTType.STRING);
    prop.setDefaultValue(db, "uuid()");

    EntityImpl doc = new EntityImpl("ClassC");

    byte[] val = doc.toStream();
    EntityImpl doc1 = new EntityImpl();
    ORecordInternal.unsetDirty(doc1);
    doc1.fromStream(val);
    doc1.deserializeFields();
    assertEquals(doc.field("name"), (String) doc1.field("name"));
  }

  @Test
  public void testDefaultValueDate() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATE);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));
    YTProperty some = classA.createProperty(db, "id", YTType.STRING);
    some.setDefaultValue(db, "uuid()");

    db.begin();
    EntityImpl doc = new EntityImpl(classA);
    EntityImpl saved = db.save(doc);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("id"));

    db.begin();
    YTResult inserted = db.command("insert into ClassA content {}").next();
    db.commit();

    EntityImpl seved1 = db.load(inserted.getIdentity().get());
    assertNotNull(seved1.field("date"));
    assertNotNull(seved1.field("id"));
    assertTrue(seved1.field("date") instanceof Date);
  }

  @Test
  public void testDefaultValueDateFromContent() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATE);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));
    YTProperty some = classA.createProperty(db, "id", YTType.STRING);
    some.setDefaultValue(db, "uuid()");

    String value = "2000-01-01 00:00:00";

    db.begin();
    EntityImpl doc = new EntityImpl(classA);
    EntityImpl saved = db.save(doc);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("id"));

    db.begin();
    YTResult inserted = db.command("insert into ClassA content {\"date\":\"" + value + "\"}")
        .next();
    db.commit();

    EntityImpl seved1 = db.load(inserted.getIdentity().get());
    assertNotNull(seved1.field("date"));
    assertNotNull(seved1.field("id"));
    assertTrue(seved1.field("date") instanceof Date);
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(seved1.field("date")), value);
  }

  @Test
  public void testDefaultValueFromJson() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATE);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    EntityImpl doc = new EntityImpl();
    doc.fromJSON("{'@class':'ClassA','other':'other'}");
    EntityImpl saved = db.save(doc);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueProvidedFromJson() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATETIME);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));

    String value1 = ODateHelper.getDateTimeFormatInstance().format(new Date());
    db.begin();
    EntityImpl doc = new EntityImpl();
    doc.fromJSON("{'@class':'ClassA','date':'" + value1 + "','other':'other'}");
    EntityImpl saved = db.save(doc);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), value1);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueMandatoryReadonlyFromJson() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATE);
    prop.setMandatory(db, true);
    prop.setReadonly(db, true);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    EntityImpl doc = new EntityImpl();
    doc.fromJSON("{'@class':'ClassA','other':'other'}");
    EntityImpl saved = db.save(doc);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueProvidedMandatoryReadonlyFromJson() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATETIME);
    prop.setMandatory(db, true);
    prop.setReadonly(db, true);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));

    String value1 = ODateHelper.getDateTimeFormatInstance().format(new Date());
    EntityImpl doc = new EntityImpl();
    doc.fromJSON("{'@class':'ClassA','date':'" + value1 + "','other':'other'}");
    db.begin();
    EntityImpl saved = db.save(doc);
    db.commit();
    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), value1);
    assertNotNull(saved.field("other"));
  }

  @Test
  public void testDefaultValueUpdateMandatoryReadonlyFromJson() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassA");

    YTProperty prop = classA.createProperty(db, "date", YTType.DATETIME);
    prop.setMandatory(db, true);
    prop.setReadonly(db, true);
    prop.setDefaultValue(db, ODateHelper.getDateTimeFormatInstance().format(new Date()));

    db.begin();
    EntityImpl doc = new EntityImpl();
    doc.fromJSON("{'@class':'ClassA','other':'other'}");
    EntityImpl saved = db.save(doc);
    db.commit();

    db.begin();
    saved = db.bindToSession(saved);
    doc = db.bindToSession(doc);

    assertNotNull(saved.field("date"));
    assertTrue(saved.field("date") instanceof Date);
    assertNotNull(saved.field("other"));
    String val = ODateHelper.getDateTimeFormatInstance().format(doc.field("date"));
    EntityImpl doc1 = new EntityImpl();
    doc1.fromJSON("{'@class':'ClassA','date':'" + val + "','other':'other1'}");
    saved.merge(doc1, true, true);

    saved = db.save(saved);
    db.commit();

    saved = db.bindToSession(saved);
    assertNotNull(saved.field("date"));
    assertEquals(ODateHelper.getDateTimeFormatInstance().format(saved.field("date")), val);
    assertEquals(saved.field("other"), "other1");
  }
}
