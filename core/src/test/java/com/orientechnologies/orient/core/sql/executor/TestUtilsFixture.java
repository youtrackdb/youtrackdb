package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import org.apache.commons.lang.RandomStringUtils;

/**
 *
 */
public class TestUtilsFixture extends DBTestBase {

  protected YTClass createClassInstance() {
    return getDBSchema().createClass(generateClassName());
  }

  protected YTClass createChildClassInstance(YTClass superclass) {
    return getDBSchema().createClass(generateClassName(), superclass);
  }

  private YTSchema getDBSchema() {
    return db.getMetadata().getSchema();
  }

  private static String generateClassName() {
    return "Class" + RandomStringUtils.randomNumeric(10);
  }
}
