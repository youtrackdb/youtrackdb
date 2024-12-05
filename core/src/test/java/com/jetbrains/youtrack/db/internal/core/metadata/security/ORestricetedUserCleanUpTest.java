package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.directmemory.ODirectMemoryAllocator;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class ORestricetedUserCleanUpTest extends DBTestBase {

  @Test
  public void testAutoCleanUserAfterDelete() {
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass("TestRecord", schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME));

    System.gc();
    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();

    OSecurity security = db.getMetadata().getSecurity();

    db.begin();
    YTUser auser = security.createUser("auser", "wherever", new String[]{});
    YTUser reader = security.getUser("admin");
    EntityImpl doc = new EntityImpl("TestRecord");
    Set<YTIdentifiable> users = new HashSet<YTIdentifiable>();
    users.add(auser.getIdentity(db));
    users.add(reader.getIdentity(db));

    doc.field(OSecurityShared.ALLOW_READ_FIELD, users);
    doc.field(OSecurityShared.ALLOW_UPDATE_FIELD, users);
    doc.field(OSecurityShared.ALLOW_DELETE_FIELD, users);
    doc.field(OSecurityShared.ALLOW_ALL_FIELD, users);
    EntityImpl rid = db.save(doc);
    db.commit();

    System.gc();
    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
    db.begin();
    security.dropUser("auser");
    db.commit();

    db.begin();
    doc = db.load(rid.getIdentity());
    Assert.assertEquals(2, ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size());

    doc.field("abc", "abc");
    doc.save();
    db.commit();

    System.gc();
    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
//
//    db.begin();
//    doc = db.load(rid.getIdentity());
//
//    ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).remove(null);
//    ((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).remove(null);
//    ((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).remove(null);
//    ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).remove(null);
//
//    Assert.assertEquals(1, ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size());
//    doc.field("abc", "abc");
//    doc.save();
//    db.commit();
//
//    System.gc();
//    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
//
//    System.gc();
//    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
  }
}
