package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import org.junit.Test;

/**
 *
 */
public class OJsonWithCustom {

  @Test
  public void testCustomField() {
    boolean old = YTGlobalConfiguration.DB_CUSTOM_SUPPORT.getValueAsBoolean();
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(true);
    YTDocument doc = new YTDocument();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    YTDocument doc1 = new YTDocument();
    doc1.fromJSON(json);
    assertEquals(doc.<String>field("test"), doc1.field("test"));
    YTGlobalConfiguration.DB_CUSTOM_SUPPORT.setValue(old);
  }

  @Test(expected = ODatabaseException.class)
  public void testCustomFieldDisabled() {
    YTDocument doc = new YTDocument();
    doc.field("test", String.class, YTType.CUSTOM);

    String json = doc.toJSON();

    System.out.println(json);

    YTDocument doc1 = new YTDocument();
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
        YTDocument doc = new YTDocument("TestCustom");
        doc.field("test", TestCustom.ONE, YTType.CUSTOM);

        String json = doc.toJSON();

        YTDocument doc1 = new YTDocument();
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
