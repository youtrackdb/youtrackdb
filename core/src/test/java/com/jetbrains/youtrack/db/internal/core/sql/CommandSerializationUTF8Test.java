package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

public class CommandSerializationUTF8Test extends DbTestBase {

  @Test
  public void testRightSerializationEncoding() {

    SQLQuery<?> query = new SQLSynchQuery<Object>("select from Profile where name='😢😂 '");

    Assert.assertEquals(query.toStream().length, 66);

    SQLQuery<?> query1 = new SQLSynchQuery<Object>();
    query1.fromStream(db,
        query.toStream(), RecordSerializerFactory.instance().getDefaultRecordSerializer());

    Assert.assertEquals(query1.getText(), "select from Profile where name='😢😂 '");
  }
}
