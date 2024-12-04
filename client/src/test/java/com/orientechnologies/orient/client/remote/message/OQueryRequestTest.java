package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OQueryRequestTest extends DBTestBase {

  @Test
  public void testWithPositionalParams() throws IOException {
    Object[] params = new Object[]{1, "Foo"};
    OQueryRequest request =
        new OQueryRequest(db,
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY, ORecordSerializerNetworkFactory.INSTANCE.current(), 123);

    MockChannel channel = new MockChannel();
    request.write(null, channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(db, channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());

    Assert.assertFalse(other.isNamedParams());
    Assert.assertArrayEquals(request.getPositionalParameters(db),
        other.getPositionalParameters(db));

    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }

  @Test
  public void testWithNamedParams() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("foo", "bar");
    params.put("baz", 12);
    OQueryRequest request =
        new OQueryRequest(db,
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY,
            ORecordSerializerNetworkFactory.INSTANCE.current(), 123);

    MockChannel channel = new MockChannel();
    request.write(null, channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(db, channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());
    Assert.assertTrue(other.isNamedParams());
    Assert.assertEquals(request.getNamedParameters(db), other.getNamedParameters(db));
    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }

  @Test
  public void testWithNoParams() throws IOException {
    Map<String, Object> params = null;
    OQueryRequest request =
        new OQueryRequest(db,
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY,
            ORecordSerializerNetworkFactory.INSTANCE.current(), 123);

    MockChannel channel = new MockChannel();
    request.write(null, channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(db, channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());
    Assert.assertTrue(other.isNamedParams());
    Assert.assertEquals(request.getNamedParameters(db), other.getNamedParameters(db));
    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }
}
