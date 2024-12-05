package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class YTResultSetTest extends DBTestBase {

  @Test
  public void testResultStream() {
    YTInternalResultSet rs = new YTInternalResultSet();
    for (int i = 0; i < 10; i++) {
      YTResultInternal item = new YTResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.stream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(45, result.get().intValue());
  }

  @Test
  public void testResultEmptyVertexStream() {
    YTInternalResultSet rs = new YTInternalResultSet();
    for (int i = 0; i < 10; i++) {
      YTResultInternal item = new YTResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testResultEdgeVertexStream() {
    YTInternalResultSet rs = new YTInternalResultSet();
    for (int i = 0; i < 10; i++) {
      YTResultInternal item = new YTResultInternal(db);
      item.setProperty("i", i);
      rs.add(item);
    }
    Optional<Integer> result =
        rs.vertexStream().map(x -> (int) x.getProperty("i")).reduce((a, b) -> a + b);
    Assert.assertFalse(result.isPresent());
  }
}
