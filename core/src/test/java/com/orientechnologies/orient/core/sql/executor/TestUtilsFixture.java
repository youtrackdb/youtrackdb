package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.apache.commons.lang.RandomStringUtils;

/**
 *
 */
public class TestUtilsFixture extends DBTestBase {

  protected OClass createClassInstance() {
    return getDBSchema().createClass(generateClassName());
  }

  protected OClass createChildClassInstance(OClass superclass) {
    return getDBSchema().createClass(generateClassName(), superclass);
  }

  private OSchema getDBSchema() {
    return db.getMetadata().getSchema();
  }

  private static String generateClassName() {
    return "Class" + RandomStringUtils.randomNumeric(10);
  }
}
