package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseSessionAbstract;
import com.jetbrains.youtrack.db.internal.core.exception.YTConfigurationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTStorageException;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
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

  @Test(expected = YTStorageException.class)
  public void createCsvDatabaseConnectBinary() throws IOException {
    ORecordSerializer prev = YTDatabaseSessionAbstract.getDefaultSerializer();
    YTDatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    createDatabase();

    YTDatabaseSessionInternal dbTx = null;
    try {
      YTDatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
      dbTx = new YTDatabaseDocumentTx("remote:localhost/test");
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
      YTDatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  private void dropDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.dropDatabase("plocal");
  }

  private void createDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "root");
    admin.createDatabase("document", "plocal");
  }

  @Test
  public void createBinaryDatabaseConnectCsv() throws IOException {
    ORecordSerializer prev = YTDatabaseSessionAbstract.getDefaultSerializer();
    YTDatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
    createDatabase();

    YTDatabaseSessionInternal dbTx = null;
    try {
      YTDatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
      dbTx = new YTDatabaseDocumentTx("remote:localhost/test");
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
      YTDatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    File directory = new File(server.getDatabaseDirectory());
    FileUtils.deleteRecursively(directory);
    YTDatabaseSessionAbstract.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    YouTrackDBManager.instance().startup();
  }

  private void deleteDirectory(File iDirectory) {
    if (iDirectory.isDirectory()) {
      for (File f : iDirectory.listFiles()) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else if (!f.delete()) {
          throw new YTConfigurationException("Cannot delete the file: " + f);
        }
      }
    }
  }
}
