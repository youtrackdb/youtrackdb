package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.document.ODatabaseSessionAbstract;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
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

  @Test(expected = OStorageException.class)
  public void createCsvDatabaseConnectBinary() throws IOException {
    ORecordSerializer prev = ODatabaseSessionAbstract.getDefaultSerializer();
    ODatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    createDatabase();

    ODatabaseSessionInternal dbTx = null;
    try {
      ODatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
      dbTx = new ODatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      dbTx.commit();
      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.<Object>field("name"), document.field("name"));
      assertEquals(doc.<Object>field("surname"), document.field("surname"));
    } finally {
      if (dbTx != null && !dbTx.isClosed()) {
        dbTx.close();
        dbTx.getStorage().close(dbTx);
      }

      dropDatabase();
      ODatabaseSessionAbstract.setDefaultSerializer(prev);
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
    ORecordSerializer prev = ODatabaseSessionAbstract.getDefaultSerializer();
    ODatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
    createDatabase();

    ODatabaseSessionInternal dbTx = null;
    try {
      ODatabaseSessionAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
      dbTx = new ODatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      dbTx.begin();
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      var fields = document.fields();
      var name = document.field("name");
      var surname = document.field("surname");
      dbTx.commit();

      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), fields);
      assertEquals(doc.field("name"), name);
      assertEquals(doc.field("surname"), surname);
    } finally {
      if (dbTx != null) {
        dbTx.close();
      }

      dropDatabase();
      ODatabaseSessionAbstract.setDefaultSerializer(prev);
    }
  }

  @After
  public void after() {
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    File directory = new File(server.getDatabaseDirectory());
    OFileUtils.deleteRecursively(directory);
    ODatabaseSessionAbstract.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    YouTrackDBManager.instance().startup();
  }

  private void deleteDirectory(File iDirectory) {
    if (iDirectory.isDirectory()) {
      for (File f : iDirectory.listFiles()) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else if (!f.delete()) {
          throw new OConfigurationException("Cannot delete the file: " + f);
        }
      }
    }
  }
}
