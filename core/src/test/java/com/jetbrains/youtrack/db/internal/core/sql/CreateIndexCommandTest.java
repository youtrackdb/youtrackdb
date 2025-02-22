package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import org.junit.Test;

/**
 *
 */
public class CreateIndexCommandTest extends DbTestBase {

  @Test(expected = IndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    db.getMetadata().getSchema().createClass("Test");
    db.command(" create index Test.test on Test(test collate ci) UNIQUE").close();
  }
}
