package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import java.util.Locale;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ODatabaseMetadataUpdateListener {

  private OxygenDB oxygenDB;
  private ODatabaseSession session;
  private int count;

  @Before
  public void before() {
    oxygenDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    session = oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    count = 0;
    OMetadataUpdateListener listener =
        new OMetadataUpdateListener() {

          @Override
          public void onSchemaUpdate(ODatabaseSessionInternal session, String database,
              OSchemaShared schema) {
            count++;
            assertNotNull(schema);
          }

          @Override
          public void onIndexManagerUpdate(ODatabaseSessionInternal session, String database,
              OIndexManagerAbstract indexManager) {
            count++;
            assertNotNull(indexManager);
          }

          @Override
          public void onFunctionLibraryUpdate(ODatabaseSessionInternal session, String database) {
            count++;
          }

          @Override
          public void onSequenceLibraryUpdate(ODatabaseSessionInternal session, String database) {
            count++;
          }

          @Override
          public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
            count++;
            assertNotNull(update);
          }
        };

    ((ODatabaseSessionInternal) session).getSharedContext().registerListener(listener);
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
          .createSequence("sequence1", OSequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (ODatabaseException exc) {
      Assert.fail("Failed to create sequence");
    }
    assertEquals(count, 1);
  }

  @Test
  public void testIndexUpdate() {
    session
        .createClass("Some")
        .createProperty(session, "test", OType.STRING)
        .createIndex(session, OClass.INDEX_TYPE.NOTUNIQUE);
    assertEquals(count, 3);
  }

  @Test
  public void testIndexConfigurationUpdate() {
    session.set(ODatabaseSession.ATTRIBUTES.LOCALECOUNTRY, Locale.GERMAN);
    assertEquals(count, 1);
  }

  @After
  public void after() {
    session.close();
    oxygenDB.close();
  }
}
