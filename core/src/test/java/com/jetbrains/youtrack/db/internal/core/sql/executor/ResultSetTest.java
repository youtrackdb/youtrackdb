package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ResultSetTest extends DbTestBase {

  @Test
  public void testResultStream() {
    var rs = new InternalResultSet();
    for (var i = 0; i < 10; i++) {
      var item = new ResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    var result =
        rs.stream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(45, result.get().intValue());
  }

  @Test
  public void testResultEmptyVertexStream() {
    var rs = new InternalResultSet();
    for (var i = 0; i < 10; i++) {
      var item = new ResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    var result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testResultEdgeVertexStream() {
    var rs = new InternalResultSet();
    for (var i = 0; i < 10; i++) {
      var item = new ResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    var result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }
}
