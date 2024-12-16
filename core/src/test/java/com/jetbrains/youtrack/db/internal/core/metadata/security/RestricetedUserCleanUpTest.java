package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.directmemory.DirectMemoryAllocator;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class RestricetedUserCleanUpTest extends DbTestBase {

  @Test
  public void testAutoCleanUserAfterDelete() {
    Schema schema = db.getMetadata().getSchema();
    schema.createClass("TestRecord", schema.getClass(SecurityShared.RESTRICTED_CLASSNAME));

    System.gc();
    DirectMemoryAllocator.instance().checkTrackedPointerLeaks();

    Security security = db.getMetadata().getSecurity();

    db.begin();
    SecurityUserIml auser = security.createUser("auser", "wherever", new String[]{});
    SecurityUserIml reader = security.getUser("admin");
    EntityImpl doc = new EntityImpl("TestRecord");
    Set<Identifiable> users = new HashSet<Identifiable>();
    users.add(auser.getIdentity(db));
    users.add(reader.getIdentity(db));

    doc.field(SecurityShared.ALLOW_READ_FIELD, users);
    doc.field(SecurityShared.ALLOW_UPDATE_FIELD, users);
    doc.field(SecurityShared.ALLOW_DELETE_FIELD, users);
    doc.field(SecurityShared.ALLOW_ALL_FIELD, users);
    EntityImpl rid = db.save(doc);
    db.commit();

    System.gc();
    DirectMemoryAllocator.instance().checkTrackedPointerLeaks();
    db.begin();
    security.dropUser("auser");
    db.commit();

    db.begin();
    doc = db.load(rid.getIdentity());
    Assert.assertEquals(2, ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(SecurityShared.ALLOW_UPDATE_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(SecurityShared.ALLOW_DELETE_FIELD)).size());
    Assert.assertEquals(2, ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).size());

    doc.field("abc", "abc");
    doc.save();
    db.commit();

    System.gc();
    DirectMemoryAllocator.instance().checkTrackedPointerLeaks();
//
//    db.begin();
//    doc = db.load(rid.getIdentity());
//
//    ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).remove(null);
//    ((Set<?>) doc.field(SecurityShared.ALLOW_UPDATE_FIELD)).remove(null);
//    ((Set<?>) doc.field(SecurityShared.ALLOW_DELETE_FIELD)).remove(null);
//    ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).remove(null);
//
//    Assert.assertEquals(1, ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(SecurityShared.ALLOW_UPDATE_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(SecurityShared.ALLOW_DELETE_FIELD)).size());
//    Assert.assertEquals(1, ((Set<?>) doc.field(SecurityShared.ALLOW_ALL_FIELD)).size());
//    doc.field("abc", "abc");
//    doc.save();
//    db.commit();
//
//    System.gc();
//    DirectMemoryAllocator.instance().checkTrackedPointerLeaks();
//
//    System.gc();
//    DirectMemoryAllocator.instance().checkTrackedPointerLeaks();
  }
}
