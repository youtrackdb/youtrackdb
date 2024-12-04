package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

public class DefaultClusterTest {

  @Test
  public void defaultClusterTest() {
    final YouTrackDB context =
        OCreateDatabaseUtil.createDatabase("test",
            DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    try (final ODatabaseSession db =
        context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      var v =
          db.computeInTx(
              () -> {
                final OVertex vertex = db.newVertex("V");
                vertex.setProperty("embedded", new ODocument());
                db.save(vertex);
                return vertex;
              });

      final ODocument embedded = db.bindToSession(v).getProperty("embedded");
      Assert.assertFalse("Found: " + embedded.getIdentity(), embedded.getIdentity().isValid());
    }
  }
}
