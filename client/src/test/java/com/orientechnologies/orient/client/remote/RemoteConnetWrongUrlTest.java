package com.orientechnologies.orient.client.remote;

import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import org.junit.Test;

public class RemoteConnetWrongUrlTest {

  @Test(expected = YTDatabaseException.class)
  public void testConnectWrongUrl() {
    YTDatabaseSessionInternal doc = new YTDatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");
  }

  @Test
  public void testConnectWrongUrlTL() {
    try {
      YTDatabaseSessionInternal doc = new YTDatabaseDocumentTx("remote:wrong:2424/test");
      doc.open("user", "user");
    } catch (YTDatabaseException e) {

    }
    assertNull(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }
}
