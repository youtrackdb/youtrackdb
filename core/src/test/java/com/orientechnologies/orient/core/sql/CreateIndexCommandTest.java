package com.orientechnologies.orient.core.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.index.YTIndexException;
import org.junit.Test;

/**
 *
 */
public class CreateIndexCommandTest extends DBTestBase {

  @Test(expected = YTIndexException.class)
  public void testCreateIndexOnMissingPropertyWithCollate() {
    db.getMetadata().getSchema().createClass("Test");
    db.command(" create index Test.test on Test(test collate ci) UNIQUE").close();
  }
}
