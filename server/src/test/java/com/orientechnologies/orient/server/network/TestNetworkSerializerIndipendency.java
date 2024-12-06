package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionAbstract;
import com.jetbrains.youtrack.db.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerSchemaAware2CSV;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNetworkSerializerIndipendency {

  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test(expected = StorageException.class)
  public void createCsvDatabaseConnectBinary() throws IOException {
    RecordSerializer prev = DatabaseSessionAbstract.getDefaultSerializer();
    DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerSchemaAware2CSV.INSTANCE);
    createDatabase();

    DatabaseSessionInternal dbTx = null;
    try {
      DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerBinary.INSTANCE);
      dbTx = new DatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      EntityImpl document = new EntityImpl();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      dbTx.commit();
      EntityImpl doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.<Object>field("name"), document.field("name"));
      assertEquals(doc.<Object>field("surname"), document.field("surname"));
    } finally {
      if (dbTx != null && !dbTx.isClosed()) {
        dbTx.close();
        dbTx.getStorage().close(dbTx);
      }

      dropDatabase();
      DatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  private void dropDatabase() throws IOException {
    ServerAdmin admin = new ServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.dropDatabase("plocal");
  }

  private void createDatabase() throws IOException {
    ServerAdmin admin = new ServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.createDatabase("document", "plocal");
  }

  @Test
  public void createBinaryDatabaseConnectCsv() throws IOException {
    RecordSerializer prev = DatabaseSessionAbstract.getDefaultSerializer();
    DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerBinary.INSTANCE);
    createDatabase();

    DatabaseSessionInternal dbTx = null;
    try {
      DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerSchemaAware2CSV.INSTANCE);
      dbTx = new DatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      dbTx.begin();
      EntityImpl document = new EntityImpl();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      var fields = document.fields();
      var name = document.field("name");
      var surname = document.field("surname");
      dbTx.commit();

      EntityImpl doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), fields);
      assertEquals(doc.field("name"), name);
      assertEquals(doc.field("surname"), surname);
    } finally {
      if (dbTx != null) {
        dbTx.close();
      }

      dropDatabase();
      DatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    File directory = new File(server.getDatabaseDirectory());
    FileUtils.deleteRecursively(directory);
    DatabaseSessionAbstract.setDefaultSerializer(
        RecordSerializerFactory.instance().getFormat(RecordSerializerBinary.NAME));
    YouTrackDBManager.instance().startup();
  }

  private void deleteDirectory(File iDirectory) {
    if (iDirectory.isDirectory()) {
      for (File f : iDirectory.listFiles()) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else if (!f.delete()) {
          throw new ConfigurationException("Cannot delete the file: " + f);
        }
      }
    }
  }
}
