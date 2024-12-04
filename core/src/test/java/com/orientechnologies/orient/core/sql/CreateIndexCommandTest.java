package com.orientechnologies.orient.core.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.index.OIndexException;
import org.junit.Test;

/**
 *
 */
public class CreateIndexCommandTest extends DBTestBase {

  @Test(expected = OIndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    db.getMetadata().getSchema().createClass("Test");
    db.command(" create index Test.test on Test(test collate ci) UNIQUE").close();
  }
}
