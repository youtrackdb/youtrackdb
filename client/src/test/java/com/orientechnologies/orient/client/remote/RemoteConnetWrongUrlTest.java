package com.orientechnologies.orient.client.remote;

import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import org.junit.Test;

public class RemoteConnetWrongUrlTest {

  @Test(expected = ODatabaseException.class)
  public void testConnectWrongUrl() {
    YTDatabaseSessionInternal doc = new YTDatabaseDocumentTx("remote:wrong:2424/test");
    doc.open("user", "user");
  }

  @Test
  public void testConnectWrongUrlTL() {
    try {
      YTDatabaseSessionInternal doc = new YTDatabaseDocumentTx("remote:wrong:2424/test");
      doc.open("user", "user");
    } catch (ODatabaseException e) {

    }
    assertNull(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }
}
