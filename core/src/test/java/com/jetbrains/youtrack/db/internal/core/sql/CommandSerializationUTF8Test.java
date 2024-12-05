package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializerFactory;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Test;

public class CommandSerializationUTF8Test extends DBTestBase {

  @Test
  public void testRightSerializationEncoding() {

    OSQLQuery<?> query = new OSQLSynchQuery<Object>("select from Profile where name='😢😂 '");

    Assert.assertEquals(query.toStream().length, 66);

    OSQLQuery<?> query1 = new OSQLSynchQuery<Object>();
    query1.fromStream(db,
        query.toStream(), ORecordSerializerFactory.instance().getDefaultRecordSerializer());

    Assert.assertEquals(query1.getText(), "select from Profile where name='😢😂 '");
  }
}
