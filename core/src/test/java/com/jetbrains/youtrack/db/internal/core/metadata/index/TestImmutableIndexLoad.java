package com.jetbrains.youtrack.db.internal.core.metadata.index;

import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class TestImmutableIndexLoad {

  @Test
  public void testLoadAndUseIndexOnOpen() {
    YouTrackDB youTrackDB =
        CreateDatabaseUtil.createDatabase(
            TestImmutableIndexLoad.class.getSimpleName(),
            DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_PLOCAL);
    DatabaseSession db =
        youTrackDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    SchemaClass one = db.createClass("One");
    SchemaProperty property = one.createProperty(db, "one", PropertyType.STRING);
    property.createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);
    db.close();
    youTrackDB.close();

    youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        youTrackDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.begin();
    EntityImpl doc = (EntityImpl) db.newEntity("One");
    doc.setProperty("one", "a");
    db.save(doc);
    db.commit();
    try {
      db.begin();
      EntityImpl doc1 = (EntityImpl) db.newEntity("One");
      doc1.setProperty("one", "a");
      db.save(doc1);
      db.commit();
      fail("It should fail the unique index");
    } catch (RecordDuplicatedException e) {
      // EXPEXTED
    }
    db.close();
    youTrackDB.drop(TestImmutableIndexLoad.class.getSimpleName());
    youTrackDB.close();
  }
}
