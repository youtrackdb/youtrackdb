package com.jetbrains.youtrack.db.internal.core.db.document;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseDocumentSTUsageTest {

  @Test
  public void testShareBetweenThreads() {
    final DatabaseDocumentTx db = new DatabaseDocumentTx("memory:DatabaseDocumentSTUsageTest");
    db.create();
    db.close();

    db.open("admin", "admin");

    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    Future<Object> future =
        singleThread.submit(
            () -> {
              db.open("admin", "admin");
              return null;
            });

    try {
      future.get();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
    }
    db.close();
  }
}
