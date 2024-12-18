package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence;
import java.util.Locale;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatabaseMetadataUpdateListener {

  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal session;
  private int count;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase("test", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    session = (DatabaseSessionInternal) youTrackDB.open("test", "admin",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    count = 0;
    MetadataUpdateListener listener =
        new MetadataUpdateListener() {

          @Override
          public void onSchemaUpdate(DatabaseSessionInternal db, String database,
              SchemaShared schema) {
            count++;
            assertNotNull(schema);
          }

          @Override
          public void onIndexManagerUpdate(DatabaseSessionInternal session, String database,
              IndexManagerAbstract indexManager) {
            count++;
            assertNotNull(indexManager);
          }

          @Override
          public void onFunctionLibraryUpdate(DatabaseSessionInternal session, String database) {
            count++;
          }

          @Override
          public void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database) {
            count++;
          }

          @Override
          public void onStorageConfigurationUpdate(String database, StorageConfiguration update) {
            count++;
            assertNotNull(update);
          }
        };

    session.getSharedContext().registerListener(listener);
  }

  @Test
  public void testSchemaUpdateListener() {
    session.createClass("test1");
    assertEquals(count, 1);
  }

  @Test
  public void testFunctionUpdateListener() {
    session.getMetadata().getFunctionLibrary().createFunction("some");
    assertEquals(count, 1);
  }

  @Test
  public void testSequenceUpdate() {
    try {
      session
          .getMetadata()
          .getSequenceLibrary()
          .createSequence("sequence1", Sequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (DatabaseException exc) {
      Assert.fail("Failed to create sequence");
    }
    assertEquals(count, 1);
  }

  @Test
  public void testIndexUpdate() {
    session
        .createClass("Some")
        .createProperty(session, "test", PropertyType.STRING)
        .createIndex(session, SchemaClass.INDEX_TYPE.NOTUNIQUE);
    assertEquals(count, 3);
  }

  @Test
  public void testIndexConfigurationUpdate() {
    session.set(DatabaseSession.ATTRIBUTES.LOCALE_COUNTRY, Locale.GERMAN);
    assertEquals(count, 1);
  }

  @After
  public void after() {
    session.close();
    youTrackDB.close();
  }
}
