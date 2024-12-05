package com.jetbrains.youtrack.db.internal.core.db.document;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
                final Vertex vertex = db.newVertex("V");
                vertex.setProperty("embedded", new EntityImpl());
                db.save(vertex);
                return vertex;
              });

      final EntityImpl embedded = db.bindToSession(v).getProperty("embedded");
      Assert.assertFalse("Found: " + embedded.getIdentity(), embedded.getIdentity().isValid());
    }
  }
}
