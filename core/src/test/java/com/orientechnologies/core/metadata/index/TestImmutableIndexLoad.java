package com.orientechnologies.core.metadata.index;

import static org.junit.Assert.fail;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.storage.YTRecordDuplicatedException;
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
    YTEntityImpl doc = new YTEntityImpl("One");
    doc.setProperty("one", "a");
    db.save(doc);
    db.commit();
    try {
      db.begin();
      YTEntityImpl doc1 = new YTEntityImpl("One");
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
