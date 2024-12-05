package com.jetbrains.youtrack.db.internal.core.db.tool;

import static com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseImport.EXPORT_IMPORT_CLASS_NAME;
import static com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseImport.EXPORT_IMPORT_INDEX_NAME;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestImportRewriteLinks {

  @Test
  public void testNestedLinkRewrite() {
    try (final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "testDB", DBTestBase.embeddedDBUrl(getClass()), OCreateDatabaseUtil.TYPE_MEMORY)) {
      try (var session =
          (YTDatabaseSessionInternal) youTrackDb.open("testDB", "admin",
              OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        final YTSchema schema = session.getMetadata().getSchema();

        final YTClass cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
        cls.createProperty(session, "key", YTType.STRING);
        cls.createProperty(session, "value", YTType.STRING);
        cls.createIndex(session, EXPORT_IMPORT_INDEX_NAME, INDEX_TYPE.UNIQUE, "key");

        session.begin();
        new EntityImpl(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new YTRecordId(10, 4).toString())
            .field("value", new YTRecordId(10, 3).toString())
            .save();

        new EntityImpl(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new YTRecordId(11, 1).toString())
            .field("value", new YTRecordId(21, 1).toString())
            .save();

        new EntityImpl(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new YTRecordId(31, 1).toString())
            .field("value", new YTRecordId(41, 1).toString())
            .save();

        new EntityImpl(EXPORT_IMPORT_CLASS_NAME)
            .field("key", new YTRecordId(51, 1).toString())
            .field("value", new YTRecordId(61, 1).toString())
            .save();
        session.commit();

        final Set<YTRID> brokenRids = new HashSet<>();

        EntityImpl doc = new EntityImpl();

        EntityImpl emb = new EntityImpl();
        doc.field("emb", emb, YTType.EMBEDDED);
        EntityImpl emb1 = new EntityImpl();
        emb.field("emb1", emb1, YTType.EMBEDDED);
        emb1.field("link", new YTRecordId(10, 4));
        emb1.field("brokenLink", new YTRecordId(10, 5));
        emb1.field("negativeLink", new YTRecordId(-1, -42));

        List<YTIdentifiable> linkList = new ArrayList<>();

        linkList.add(new YTRecordId(-1, -42));
        linkList.add(new YTRecordId(11, 2));
        linkList.add(new YTRecordId(11, 1));

        brokenRids.add(new YTRecordId(10, 5));
        brokenRids.add(new YTRecordId(11, 2));
        brokenRids.add(new YTRecordId(31, 2));
        brokenRids.add(new YTRecordId(51, 2));

        Set<YTIdentifiable> linkSet = new HashSet<>();

        linkSet.add(new YTRecordId(-1, -42));
        linkSet.add(new YTRecordId(31, 2));
        linkSet.add(new YTRecordId(31, 1));

        Map<String, YTIdentifiable> linkMap = new HashMap<>();

        linkMap.put("key1", new YTRecordId(51, 1));
        linkMap.put("key2", new YTRecordId(51, 2));
        linkMap.put("key3", new YTRecordId(-1, -42));

        emb1.field("linkList", linkList);
        emb1.field("linkSet", linkSet);
        emb1.field("linkMap", linkMap);

        ODatabaseImport.doRewriteLinksInDocument(session, doc,
            brokenRids);

        Assert.assertEquals(new YTRecordId(10, 3), emb1.getLinkProperty("link"));
        Assert.assertEquals(new YTRecordId(-1, -42), emb1.getLinkProperty("negativeLink"));
        Assert.assertNull(emb1.field("brokenLink"));

        List<YTIdentifiable> resLinkList = new ArrayList<>();
        resLinkList.add(new YTRecordId(-1, -42));
        resLinkList.add(new YTRecordId(21, 1));

        Assert.assertEquals(emb1.field("linkList"), resLinkList);

        Set<YTIdentifiable> resLinkSet = new HashSet<>();
        resLinkSet.add(new YTRecordId(41, 1));
        resLinkSet.add(new YTRecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkSet"), resLinkSet);

        Map<String, YTIdentifiable> resLinkMap = new HashMap<>();
        resLinkMap.put("key1", new YTRecordId(61, 1));
        resLinkMap.put("key3", new YTRecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkMap"), resLinkMap);
      }
    }
  }
}
