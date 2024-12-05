package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import org.junit.Assert;
import org.junit.Test;

public class DefaultClusterTest {

  @Test
  public void defaultClusterTest() {
    final YouTrackDB context =
        OCreateDatabaseUtil.createDatabase("test",
            DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    try (final YTDatabaseSession db =
        context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      var v =
          db.computeInTx(
              () -> {
                final YTVertex vertex = db.newVertex("V");
                vertex.setProperty("embedded", new YTEntityImpl());
                db.save(vertex);
                return vertex;
              });

      final YTEntityImpl embedded = db.bindToSession(v).getProperty("embedded");
      Assert.assertFalse("Found: " + embedded.getIdentity(), embedded.getIdentity().isValid());
    }
  }
}
