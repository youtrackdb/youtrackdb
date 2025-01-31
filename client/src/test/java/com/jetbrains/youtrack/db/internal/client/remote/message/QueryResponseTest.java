package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkFactory;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class QueryResponseTest extends DbTestBase {

  @Test
  public void test() throws IOException {

    List<Result> resuls = new ArrayList<>();
    for (var i = 0; i < 10; i++) {
      var item = new ResultInternal(db);
      item.setProperty("name", "foo");
      item.setProperty("counter", i);
      resuls.add(item);
    }
    var response =
        new QueryResponse("query", true, resuls, Optional.empty(), false, new HashMap<>(), true);

    var channel = new MockChannel();
    response.write(null,
        channel,
        ChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION,
        RecordSerializerNetworkFactory.INSTANCE.current());

    channel.close();

    var newResponse = new QueryResponse();

    newResponse.read(db, channel, null);
    var responseRs = newResponse.getResult().iterator();

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(responseRs.hasNext());
      var item = responseRs.next();
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals((Integer) i, item.getProperty("counter"));
    }
    Assert.assertFalse(responseRs.hasNext());
    Assert.assertTrue(newResponse.isReloadMetadata());
    Assert.assertTrue(newResponse.isTxChanges());
  }
}
