package com.jetbrains.youtrack.db.internal.client.remote.message;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.client.remote.message.live.LiveQueryResult;
import com.jetbrains.youtrack.db.internal.common.exception.ErrorCode;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 *
 */
public class LiveQueryMessagesTests extends DbTestBase {

  @Test
  public void testRequestWriteRead() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("par", "value");
    SubscribeLiveQueryRequest request = new SubscribeLiveQueryRequest("select from Some", params);
    MockChannel channel = new MockChannel();
    request.write(null, channel, null);
    channel.close();
    SubscribeLiveQueryRequest requestRead = new SubscribeLiveQueryRequest();
    requestRead.read(db, channel, -1, new RecordSerializerNetworkV37());
    assertEquals("select from Some", requestRead.getQuery());
    assertEquals(requestRead.getParams(), params);
  }

  @Test
  public void testSubscribeResponseWriteRead() throws IOException {
    SubscribeLiveQueryResponse response = new SubscribeLiveQueryResponse(20);
    MockChannel channel = new MockChannel();
    response.write(null, channel, 0, null);
    channel.close();
    SubscribeLiveQueryResponse responseRead = new SubscribeLiveQueryResponse();
    responseRead.read(db, channel, null);
    assertEquals(20, responseRead.getMonitorId());
  }

  @Test
  public void testLiveQueryErrorPushRequest() throws IOException {

    LiveQueryPushRequest pushRequest =
        new LiveQueryPushRequest(10, 20, ErrorCode.GENERIC_ERROR, "the message");
    MockChannel channel = new MockChannel();
    pushRequest.write(null, channel);
    channel.close();
    LiveQueryPushRequest pushRequestRead = new LiveQueryPushRequest();
    pushRequestRead.read(db, channel);
    assertEquals(10, pushRequestRead.getMonitorId());
    assertEquals(LiveQueryPushRequest.ERROR, pushRequestRead.getStatus());
    assertEquals(20, pushRequestRead.getErrorIdentifier());
    assertEquals(ErrorCode.GENERIC_ERROR, pushRequestRead.getErrorCode());
    assertEquals("the message", pushRequestRead.getErrorMessage());
  }

  @Test
  public void testLiveQueryPushRequest() throws IOException {

    List<LiveQueryResult> events = new ArrayList<>();
    ResultInternal res = new ResultInternal(db);
    res.setProperty("one", "one");
    res.setProperty("two", 10);
    events.add(new LiveQueryResult(LiveQueryResult.CREATE_EVENT, res, null));
    events.add(
        new LiveQueryResult(
            LiveQueryResult.UPDATE_EVENT, new ResultInternal(db), new ResultInternal(db)));
    events.add(new LiveQueryResult(LiveQueryResult.DELETE_EVENT, new ResultInternal(db), null));

    LiveQueryPushRequest pushRequest =
        new LiveQueryPushRequest(10, LiveQueryPushRequest.END, events);
    MockChannel channel = new MockChannel();
    pushRequest.write(null, channel);
    channel.close();
    LiveQueryPushRequest pushRequestRead = new LiveQueryPushRequest();
    pushRequestRead.read(db, channel);

    assertEquals(10, pushRequestRead.getMonitorId());
    assertEquals(LiveQueryPushRequest.END, pushRequestRead.getStatus());
    assertEquals(3, pushRequestRead.getEvents().size());
    assertEquals("one", pushRequestRead.getEvents().get(0).getCurrentValue().getProperty("one"));
    assertEquals(10, (int) pushRequestRead.getEvents().get(0).getCurrentValue().getProperty("two"));
  }
}
