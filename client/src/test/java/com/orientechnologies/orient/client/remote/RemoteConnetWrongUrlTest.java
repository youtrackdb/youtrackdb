package com.orientechnologies.orient.client.remote;

import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import org.junit.Test;

public class RemoteConnetWrongUrlTest {

  @Test(expected = DatabaseException.class)
  public void testConnectWrongUrl() {
    DatabaseSessionInternal doc = new DatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");
  }

  @Test
  public void testConnectWrongUrlTL() {
    try {
      DatabaseSessionInternal doc = new DatabaseDocumentTx("remote:wrong:2424/test");
      doc.open("user", "user");
    } catch (DatabaseException e) {

    }
    assertNull(DatabaseRecordThreadLocal.instance().getIfDefined());
  }
}
