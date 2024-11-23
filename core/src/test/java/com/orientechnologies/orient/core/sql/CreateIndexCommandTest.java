package com.orientechnologies.orient.core.sql;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.index.OIndexException;
import org.junit.Test;

/**
 *
 */
public class CreateIndexCommandTest extends BaseMemoryDatabase {

  @Test(expected = OIndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    db.getMetadata().getSchema().createClass("Test");
    db.command(" create index Test.test on Test(test collate ci) UNIQUE").close();
  }
}
