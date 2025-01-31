package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import org.junit.Test;

/**
 *
 */
public class JsonWithCustom extends DbTestBase {

  @Test
  public void testCustomField() {
    var old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    var doc = (EntityImpl) db.newEntity();
    doc.field("test", String.class, PropertyType.CUSTOM);

    var json = doc.toJSON();

    System.out.println(json);

    var doc1 = (EntityImpl) db.newEntity();
    doc1.updateFromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test(expected = DatabaseException.class)
  public void testCustomFieldDisabled() {
    var doc = (EntityImpl) db.newEntity();
    doc.field("test", String.class, PropertyType.CUSTOM);

    var json = doc.toJSON();

    System.out.println(json);

    var doc1 = (EntityImpl) db.newEntity();
    doc1.updateFromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
  }

  @Test
  public void testCustomSerialization() {
    var old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    try (final YouTrackDB youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "testJson", DbTestBase.embeddedDBUrl(getClass()), CreateDatabaseUtil.TYPE_MEMORY)) {
      // youTrackDB.create("testJson", DatabaseType.MEMORY);
      try (var db = (DatabaseSessionInternal) youTrackDB.open("testJson", "admin",
          CreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        var klass = db.getMetadata().getSchema().createClass("TestCustom");
        klass.createProperty(db, "test", PropertyType.CUSTOM);
        var doc = (EntityImpl) db.newEntity("TestCustom");
        doc.field("test", TestCustom.ONE, PropertyType.CUSTOM);

        var json = doc.toJSON();

        var doc1 = (EntityImpl) db.newEntity();
        doc1.updateFromJSON(json);
        assertEquals(TestCustom.ONE, TestCustom.valueOf(doc1.field("test")));
      }
    }
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  public enum TestCustom {
    ONE,
    TWO
  }
}
