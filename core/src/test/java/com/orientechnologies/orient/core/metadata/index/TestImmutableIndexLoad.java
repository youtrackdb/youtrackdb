package com.orientechnologies.orient.core.metadata.index;

import static org.junit.Assert.fail;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
import org.junit.Test;

public class TestImmutableIndexLoad {

  @Test
  public void testLoadAndUseIndexOnOpen() {
    YouTrackDB youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            TestImmutableIndexLoad.class.getSimpleName(),
            DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL);
    YTDatabaseSession db =
        youTrackDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    YTClass one = db.createClass("One");
    YTProperty property = one.createProperty(db, "one", YTType.STRING);
    property.createIndex(db, YTClass.INDEX_TYPE.UNIQUE);
    db.close();
    youTrackDB.close();

    youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        youTrackDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.begin();
    YTDocument doc = new YTDocument("One");
    doc.setProperty("one", "a");
    db.save(doc);
    db.commit();
    try {
      db.begin();
      YTDocument doc1 = new YTDocument("One");
      doc1.setProperty("one", "a");
      db.save(doc1);
      db.commit();
      fail("It should fail the unique index");
    } catch (YTRecordDuplicatedException e) {
      // EXPEXTED
    }
    db.close();
    youTrackDB.drop(TestImmutableIndexLoad.class.getSimpleName());
    youTrackDB.close();
  }
}
