package com.jetbrains.youtrack.db.internal.client.remote;

import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.api.YourTracks;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import org.junit.Test;

public class RemoteConnectWrongUrlTest {

  @Test(expected = DatabaseException.class)
  public void testConnectWrongUrl() {
    try (var remote = YourTracks.remote("remote:wrong:2424", "root", "root")) {
      try (var session = remote.open("test", "admin", "admin")) {
        // do nothing
      }
    }
    assertNull(DatabaseRecordThreadLocal.instance().getIfDefined());
  }
}
