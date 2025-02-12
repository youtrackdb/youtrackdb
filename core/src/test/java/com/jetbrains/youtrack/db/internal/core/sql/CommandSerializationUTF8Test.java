package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

public class CommandSerializationUTF8Test extends DbTestBase {
  @Test
  public void testRightSerializationEncoding() {
    SQLQuery<?> query = new SQLSynchQuery<>("select from Profile where name='😢😂 '");
    Assert.assertEquals(66, query.toStream(session, RecordSerializerNetworkV37.INSTANCE).length);

    SQLQuery<?> query1 = new SQLSynchQuery<>();
    query1.fromStream(session,
        query.toStream(session, RecordSerializerNetworkV37.INSTANCE),
        RecordSerializerNetworkV37.INSTANCE);

    Assert.assertEquals("select from Profile where name='😢😂 '", query1.getText());
  }
}
