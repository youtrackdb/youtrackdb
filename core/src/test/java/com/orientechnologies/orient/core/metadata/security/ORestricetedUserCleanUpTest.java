package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class ORestricetedUserCleanUpTest extends BaseMemoryDatabase {

  @Test
  public void testAutoCleanUserAfterDelete() {
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass("TestRecord", schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME));

    System.gc();
    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();

    OSecurity security = db.getMetadata().getSecurity();

    db.begin();
    OUser auser = security.createUser("auser", "wherever", new String[]{});
    OUser reader = security.getUser("admin");
    ODocument doc = new ODocument("TestRecord");
    Set<OIdentifiable> users = new HashSet<OIdentifiable>();
    users.add(auser.getIdentity(db));
    users.add(reader.getIdentity(db));

    doc.field(OSecurityShared.ALLOW_READ_FIELD, users);
    doc.field(OSecurityShared.ALLOW_UPDATE_FIELD, users);
    doc.field(OSecurityShared.ALLOW_DELETE_FIELD, users);
    doc.field(OSecurityShared.ALLOW_ALL_FIELD, users);
    ODocument rid = db.save(doc);
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
