package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseSessionInternal> {

  protected DocumentDBBaseTest() {}

  @Parameters(value = "remote")
  protected DocumentDBBaseTest(boolean remote) {
    super(remote);
  }

  public DocumentDBBaseTest(boolean remote, String prefix) {
    super(remote, prefix);
  }

  @Override
  protected ODatabaseSessionInternal createSessionInstance(
      OrientDB orientDB, String dbName, String user, String password) {
    var session = orientDB.open(dbName, user, password);
    return (ODatabaseSessionInternal) session;
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db, Object... args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db, Map args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db) {
    return db.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Object... args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Map<?, ?> args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql) {
    return database.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }
}
