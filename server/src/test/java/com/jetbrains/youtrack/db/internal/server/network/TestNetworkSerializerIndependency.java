package com.jetbrains.youtrack.db.internal.server.network;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.YourTracks;
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionAbstract;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerSchemaAware2CSV;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNetworkSerializerIndependency {

  private YouTrackDBServer server;

  @Before
  public void before() throws Exception {
    server = new YouTrackDBServer(false);
    server.startup(getClass().getResourceAsStream("youtrackdb-server-config.xml"));
    server.activate();
  }

  @Test(expected = StorageException.class)
  public void createCsvDatabaseConnectBinary() throws IOException {
    var prev = DatabaseSessionAbstract.getDefaultSerializer();
    DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerSchemaAware2CSV.INSTANCE);
    createDatabase();

    DatabaseSessionInternal db = null;
    try {
      DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerBinary.INSTANCE);
      db = new DatabaseDocumentTx("remote:localhost/test");
      db.open("admin", "admin");
      var document = ((EntityImpl) db.newEntity());
      document.field("name", "something");
      document.field("surname", "something-else");
      document = db.save(document, db.getClusterNameById(db.getDefaultClusterId()));
      db.commit();
      EntityImpl doc = db.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.<Object>field("name"), document.field("name"));
      assertEquals(doc.<Object>field("surname"), document.field("surname"));
    } finally {
      if (db != null && !db.isClosed()) {
        db.close();
        db.getStorage().close(db);
      }

      dropDatabase();
      DatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  private void dropDatabase() throws IOException {
    var admin = new ServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.dropDatabase("plocal");
  }

  private void createDatabase() throws IOException {
    var admin = new ServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.createDatabase("document", "plocal");
  }

  @Test
  public void createBinaryDatabaseConnectCsv() throws IOException {
    var prev = DatabaseSessionAbstract.getDefaultSerializer();
    DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerBinary.INSTANCE);
    createDatabase();

    try {
      DatabaseSessionAbstract.setDefaultSerializer(RecordSerializerSchemaAware2CSV.INSTANCE);
      try (var youTrackDBManager = YourTracks.remote("remote:localhost", "root", "root")) {
        youTrackDBManager.createIfNotExists("test", DatabaseType.MEMORY, "admin", "admin", "admin");
        try (var db = youTrackDBManager.open("test", "admin", "admin")) {
          db.begin();
          var entity = ((EntityImpl) db.newEntity());
          entity.field("name", "something");
          entity.field("surname", "something-else");
          entity = db.save(entity);
          var fields = entity.fields();
          var name = entity.field("name");
          var surname = entity.field("surname");
          db.commit();

          EntityImpl doc = db.load(entity.getIdentity());
          assertEquals(doc.fields(), fields);
          assertEquals(doc.field("name"), name);
          assertEquals(doc.field("surname"), surname);
        }
      }

    } finally {
      dropDatabase();
      DatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    var directory = new File(server.getDatabaseDirectory());
    FileUtils.deleteRecursively(directory);
    DatabaseSessionAbstract.setDefaultSerializer(
        RecordSerializerFactory.instance().getFormat(RecordSerializerBinary.NAME));
    YouTrackDBEnginesManager.instance().startup();
  }

  private void deleteDirectory(File iDirectory) {
    if (iDirectory.isDirectory()) {
      for (var f : iDirectory.listFiles()) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else if (!f.delete()) {
          throw new ConfigurationException("Cannot delete the file: " + f);
        }
      }
    }
  }
}
