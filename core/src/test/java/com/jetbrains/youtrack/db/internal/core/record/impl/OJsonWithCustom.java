package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import org.junit.Test;

/**
 *
 */
public class OJsonWithCustom {

  @Test
  public void testCustomField() {
    boolean old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    EntityImpl doc = new EntityImpl();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    EntityImpl doc1 = new EntityImpl();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test(expected = YTDatabaseException.class)
  public void testCustomFieldDisabled() {
    EntityImpl doc = new EntityImpl();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    EntityImpl doc1 = new EntityImpl();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
  }

  @Test
  public void testCustomSerialization() {
    boolean old = GlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    GlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    try (final YouTrackDB youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            "testJson", DBTestBase.embeddedDBUrl(getClass()), OCreateDatabaseUtil.TYPE_MEMORY)) {
      // youTrackDB.create("testJson", ODatabaseType.MEMORY);
      try (var db = (YTDatabaseSessionInternal) youTrackDB.open("testJson", "admin",
          OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        YTClass klass = db.getMetadata().getSchema().createClass("TestCustom");
        klass.createProperty(db, "test", YTType.CUSTOM);
        EntityImpl doc = new EntityImpl("TestCustom");
        doc.field("test", TestCustom.ONE, YTType.CUSTOM);

        String json = doc.toJSON();

        EntityImpl doc1 = new EntityImpl();
        doc1.fromJSON(json);
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
