package com.orientechnologies.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import org.junit.Test;

/**
 *
 */
public class OJsonWithCustom {

  @Test
  public void testCustomField() {
    boolean old = YTGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test(expected = YTDatabaseException.class)
  public void testCustomFieldDisabled() {
    YTEntityImpl doc = new YTEntityImpl();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
  }

  @Test
  public void testCustomSerialization() {
    boolean old = YTGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    try (final YouTrackDB youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            "testJson", DBTestBase.embeddedDBUrl(getClass()), OCreateDatabaseUtil.TYPE_MEMORY)) {
      // youTrackDB.create("testJson", ODatabaseType.MEMORY);
      try (var db = (YTDatabaseSessionInternal) youTrackDB.open("testJson", "admin",
          OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        YTClass klass = db.getMetadata().getSchema().createClass("TestCustom");
        klass.createProperty(db, "test", YTType.CUSTOM);
        YTEntityImpl doc = new YTEntityImpl("TestCustom");
        doc.field("test", TestCustom.ONE, YTType.CUSTOM);

        String json = doc.toJSON();

        YTEntityImpl doc1 = new YTEntityImpl();
        doc1.fromJSON(json);
        assertEquals(TestCustom.ONE, TestCustom.valueOf(doc1.field("test")));
      }
    }
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  public enum TestCustom {
    ONE,
    TWO
  }
}
