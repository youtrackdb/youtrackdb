package com.jetbrains.youtrack.db.internal.core.db.tool;

import static com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport.EXPORT_IMPORT_CLASS_NAME;
import static com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport.EXPORT_IMPORT_INDEX_NAME;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
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
        CreateDatabaseUtil.createDatabase(
            "testDB", DbTestBase.embeddedDBUrl(getClass()), CreateDatabaseUtil.TYPE_MEMORY)) {
      try (var db =
          (DatabaseSessionInternal) youTrackDb.open("testDB", "admin",
              CreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        final Schema schema = db.getMetadata().getSchema();

        final SchemaClass cls = schema.createClass(EXPORT_IMPORT_CLASS_NAME);
        cls.createProperty(db, "key", PropertyType.STRING);
        cls.createProperty(db, "value", PropertyType.STRING);
        cls.createIndex(db, EXPORT_IMPORT_INDEX_NAME, INDEX_TYPE.UNIQUE, "key");

        db.begin();
        ((EntityImpl) db.newEntity(EXPORT_IMPORT_CLASS_NAME))
            .field("key", new RecordId(10, 4).toString())
            .field("value", new RecordId(10, 3).toString())
            .save();

        ((EntityImpl) db.newEntity(EXPORT_IMPORT_CLASS_NAME))
            .field("key", new RecordId(11, 1).toString())
            .field("value", new RecordId(21, 1).toString())
            .save();

        ((EntityImpl) db.newEntity(EXPORT_IMPORT_CLASS_NAME))
            .field("key", new RecordId(31, 1).toString())
            .field("value", new RecordId(41, 1).toString())
            .save();

        ((EntityImpl) db.newEntity(EXPORT_IMPORT_CLASS_NAME))
            .field("key", new RecordId(51, 1).toString())
            .field("value", new RecordId(61, 1).toString())
            .save();
        db.commit();

        final Set<RID> brokenRids = new HashSet<>();

        EntityImpl doc = (EntityImpl) db.newEntity();

        EntityImpl emb = (EntityImpl) db.newEntity();
        doc.field("emb", emb, PropertyType.EMBEDDED);
        EntityImpl emb1 = (EntityImpl) db.newEntity();
        emb.field("emb1", emb1, PropertyType.EMBEDDED);
        emb1.field("link", new RecordId(10, 4));
        emb1.field("brokenLink", new RecordId(10, 5));
        emb1.field("negativeLink", new RecordId(-1, -42));

        List<Identifiable> linkList = new ArrayList<>();

        linkList.add(new RecordId(-1, -42));
        linkList.add(new RecordId(11, 2));
        linkList.add(new RecordId(11, 1));

        brokenRids.add(new RecordId(10, 5));
        brokenRids.add(new RecordId(11, 2));
        brokenRids.add(new RecordId(31, 2));
        brokenRids.add(new RecordId(51, 2));

        Set<Identifiable> linkSet = new HashSet<>();

        linkSet.add(new RecordId(-1, -42));
        linkSet.add(new RecordId(31, 2));
        linkSet.add(new RecordId(31, 1));

        Map<String, Identifiable> linkMap = new HashMap<>();

        linkMap.put("key1", new RecordId(51, 1));
        linkMap.put("key2", new RecordId(51, 2));
        linkMap.put("key3", new RecordId(-1, -42));

        emb1.field("linkList", linkList);
        emb1.field("linkSet", linkSet);
        emb1.field("linkMap", linkMap);

        DatabaseImport.doRewriteLinksInDocument(db, doc,
            brokenRids);

        Assert.assertEquals(new RecordId(10, 3), emb1.getLinkProperty("link"));
        Assert.assertEquals(new RecordId(-1, -42), emb1.getLinkProperty("negativeLink"));
        Assert.assertNull(emb1.field("brokenLink"));

        List<Identifiable> resLinkList = new ArrayList<>();
        resLinkList.add(new RecordId(-1, -42));
        resLinkList.add(new RecordId(21, 1));

        Assert.assertEquals(emb1.field("linkList"), resLinkList);

        Set<Identifiable> resLinkSet = new HashSet<>();
        resLinkSet.add(new RecordId(41, 1));
        resLinkSet.add(new RecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkSet"), resLinkSet);

        Map<String, Identifiable> resLinkMap = new HashMap<>();
        resLinkMap.put("key1", new RecordId(61, 1));
        resLinkMap.put("key3", new RecordId(-1, -42));

        Assert.assertEquals(emb1.field("linkMap"), resLinkMap);
      }
    }
  }
}
