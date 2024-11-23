package com.orientechnologies.orient.core.metadata.index;

import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Test;

public class TestImmutableIndexLoad {

  @Test
  public void testLoadAndUseIndexOnOpen() {
    OxygenDB oxygenDB =
        OCreateDatabaseUtil.createDatabase(
            TestImmutableIndexLoad.class.getSimpleName(),
            "embedded:./target/",
            OCreateDatabaseUtil.TYPE_PLOCAL);
    ODatabaseSession db =
        oxygenDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    OClass one = db.createClass("One");
    OProperty property = one.createProperty(db, "one", OType.STRING);
    property.createIndex(db, OClass.INDEX_TYPE.UNIQUE);
    db.close();
    oxygenDB.close();

    oxygenDB =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        oxygenDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.begin();
    ODocument doc = new ODocument("One");
    doc.setProperty("one", "a");
    db.save(doc);
    db.commit();
    try {
      db.begin();
      ODocument doc1 = new ODocument("One");
      doc1.setProperty("one", "a");
      db.save(doc1);
      db.commit();
      fail("It should fail the unique index");
    } catch (ORecordDuplicatedException e) {
      // EXPEXTED
    }
    db.close();
    oxygenDB.drop(TestImmutableIndexLoad.class.getSimpleName());
    oxygenDB.close();
  }
}
