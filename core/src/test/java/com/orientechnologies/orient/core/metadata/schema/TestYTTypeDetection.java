package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OList;
import com.orientechnologies.orient.core.db.record.OMap;
import com.orientechnologies.orient.core.db.record.OSet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.YTSerializationException;
import com.orientechnologies.orient.core.id.ChangeableRecordId;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class TestYTTypeDetection extends DBTestBase {

  @Test
  public void testOTypeFromClass() {

    assertEquals(YTType.BOOLEAN, YTType.getTypeByClass(Boolean.class));

    assertEquals(YTType.BOOLEAN, YTType.getTypeByClass(Boolean.TYPE));

    assertEquals(YTType.LONG, YTType.getTypeByClass(Long.class));

    assertEquals(YTType.LONG, YTType.getTypeByClass(Long.TYPE));

    assertEquals(YTType.INTEGER, YTType.getTypeByClass(Integer.class));

    assertEquals(YTType.INTEGER, YTType.getTypeByClass(Integer.TYPE));

    assertEquals(YTType.SHORT, YTType.getTypeByClass(Short.class));

    assertEquals(YTType.SHORT, YTType.getTypeByClass(Short.TYPE));

    assertEquals(YTType.FLOAT, YTType.getTypeByClass(Float.class));

    assertEquals(YTType.FLOAT, YTType.getTypeByClass(Float.TYPE));

    assertEquals(YTType.DOUBLE, YTType.getTypeByClass(Double.class));

    assertEquals(YTType.DOUBLE, YTType.getTypeByClass(Double.TYPE));

    assertEquals(YTType.BYTE, YTType.getTypeByClass(Byte.class));

    assertEquals(YTType.BYTE, YTType.getTypeByClass(Byte.TYPE));

    assertEquals(YTType.STRING, YTType.getTypeByClass(Character.class));

    assertEquals(YTType.STRING, YTType.getTypeByClass(Character.TYPE));

    assertEquals(YTType.STRING, YTType.getTypeByClass(String.class));

    // assertEquals(YTType.BINARY, YTType.getTypeByClass(Byte[].class));

    assertEquals(YTType.BINARY, YTType.getTypeByClass(byte[].class));

    assertEquals(YTType.DATETIME, YTType.getTypeByClass(Date.class));

    assertEquals(YTType.DECIMAL, YTType.getTypeByClass(BigDecimal.class));

    assertEquals(YTType.INTEGER, YTType.getTypeByClass(BigInteger.class));

    assertEquals(YTType.LINK, YTType.getTypeByClass(YTIdentifiable.class));

    assertEquals(YTType.LINK, YTType.getTypeByClass(YTRecordId.class));

    assertEquals(YTType.LINK, YTType.getTypeByClass(YTRecord.class));

    assertEquals(YTType.LINK, YTType.getTypeByClass(YTDocument.class));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByClass(ArrayList.class));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByClass(List.class));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByClass(OTrackedList.class));

    assertEquals(YTType.EMBEDDEDSET, YTType.getTypeByClass(Set.class));

    assertEquals(YTType.EMBEDDEDSET, YTType.getTypeByClass(HashSet.class));

    assertEquals(YTType.EMBEDDEDSET, YTType.getTypeByClass(OTrackedSet.class));

    assertEquals(YTType.EMBEDDEDMAP, YTType.getTypeByClass(Map.class));

    assertEquals(YTType.EMBEDDEDMAP, YTType.getTypeByClass(HashMap.class));

    assertEquals(YTType.EMBEDDEDMAP, YTType.getTypeByClass(OTrackedMap.class));

    assertEquals(YTType.LINKSET, YTType.getTypeByClass(OSet.class));

    assertEquals(YTType.LINKLIST, YTType.getTypeByClass(OList.class));

    assertEquals(YTType.LINKMAP, YTType.getTypeByClass(OMap.class));

    assertEquals(YTType.LINKBAG, YTType.getTypeByClass(ORidBag.class));

    assertEquals(YTType.CUSTOM, YTType.getTypeByClass(OSerializableStream.class));

    assertEquals(YTType.CUSTOM, YTType.getTypeByClass(CustomClass.class));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByClass(Object[].class));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByClass(String[].class));

    assertEquals(YTType.EMBEDDED, YTType.getTypeByClass(ODocumentSerializable.class));

    assertEquals(YTType.EMBEDDED, YTType.getTypeByClass(DocumentSer.class));

    assertEquals(YTType.CUSTOM, YTType.getTypeByClass(ClassSerializable.class));
  }

  @Test
  public void testOTypeFromValue() {

    assertEquals(YTType.BOOLEAN, YTType.getTypeByValue(true));

    assertEquals(YTType.LONG, YTType.getTypeByValue(2L));

    assertEquals(YTType.INTEGER, YTType.getTypeByValue(2));

    assertEquals(YTType.SHORT, YTType.getTypeByValue((short) 4));

    assertEquals(YTType.FLOAT, YTType.getTypeByValue(0.5f));

    assertEquals(YTType.DOUBLE, YTType.getTypeByValue(0.7d));

    assertEquals(YTType.BYTE, YTType.getTypeByValue((byte) 10));

    assertEquals(YTType.STRING, YTType.getTypeByValue('a'));

    assertEquals(YTType.STRING, YTType.getTypeByValue("yaaahooooo"));

    assertEquals(YTType.BINARY, YTType.getTypeByValue(new byte[]{0, 1, 2}));

    assertEquals(YTType.DATETIME, YTType.getTypeByValue(new Date()));

    assertEquals(YTType.DECIMAL, YTType.getTypeByValue(new BigDecimal(10)));

    assertEquals(YTType.INTEGER, YTType.getTypeByValue(new BigInteger("20")));

    assertEquals(YTType.LINK, YTType.getTypeByValue(new YTDocument()));

    assertEquals(YTType.LINK, YTType.getTypeByValue(new ChangeableRecordId()));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByValue(new ArrayList<Object>()));

    assertEquals(
        YTType.EMBEDDEDLIST, YTType.getTypeByValue(new OTrackedList<Object>(new YTDocument())));

    assertEquals(YTType.EMBEDDEDSET, YTType.getTypeByValue(new HashSet<Object>()));

    assertEquals(YTType.EMBEDDEDMAP, YTType.getTypeByValue(new HashMap<Object, Object>()));

    assertEquals(YTType.LINKSET, YTType.getTypeByValue(new OSet(new YTDocument())));

    assertEquals(YTType.LINKLIST, YTType.getTypeByValue(new OList(new YTDocument())));

    assertEquals(YTType.LINKMAP, YTType.getTypeByValue(new OMap(new YTDocument())));

    assertEquals(YTType.LINKBAG, YTType.getTypeByValue(new ORidBag(db)));

    assertEquals(YTType.CUSTOM, YTType.getTypeByValue(new CustomClass()));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByValue(new Object[]{}));

    assertEquals(YTType.EMBEDDEDLIST, YTType.getTypeByValue(new String[]{}));

    assertEquals(YTType.EMBEDDED, YTType.getTypeByValue(new DocumentSer()));

    assertEquals(YTType.CUSTOM, YTType.getTypeByValue(new ClassSerializable()));
  }

  @Test
  public void testOTypeFromValueInternal() {
    ODatabaseRecordThreadLocal.instance().remove();

    Map<String, YTRecordId> linkmap = new HashMap<String, YTRecordId>();
    linkmap.put("some", new ChangeableRecordId());
    assertEquals(YTType.LINKMAP, YTType.getTypeByValue(linkmap));

    Map<String, YTRecord> linkmap2 = new HashMap<String, YTRecord>();
    linkmap2.put("some", new YTDocument());
    assertEquals(YTType.LINKMAP, YTType.getTypeByValue(linkmap2));

    List<YTRecordId> linkList = new ArrayList<YTRecordId>();
    linkList.add(new ChangeableRecordId());
    assertEquals(YTType.LINKLIST, YTType.getTypeByValue(linkList));

    List<YTRecord> linkList2 = new ArrayList<YTRecord>();
    linkList2.add(new YTDocument());
    assertEquals(YTType.LINKLIST, YTType.getTypeByValue(linkList2));

    Set<YTRecordId> linkSet = new HashSet<YTRecordId>();
    linkSet.add(new ChangeableRecordId());
    assertEquals(YTType.LINKSET, YTType.getTypeByValue(linkSet));

    Set<YTRecord> linkSet2 = new HashSet<YTRecord>();
    linkSet2.add(new YTDocument());
    assertEquals(YTType.LINKSET, YTType.getTypeByValue(linkSet2));

    YTDocument document = new YTDocument();
    ODocumentInternal.addOwner(document, new YTDocument());
    assertEquals(YTType.EMBEDDED, YTType.getTypeByValue(document));
  }

  public class CustomClass implements OSerializableStream {

    @Override
    public byte[] toStream() throws YTSerializationException {
      return null;
    }

    @Override
    public OSerializableStream fromStream(byte[] iStream) throws YTSerializationException {
      return null;
    }
  }

  public class DocumentSer implements ODocumentSerializable {

    @Override
    public YTDocument toDocument() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void fromDocument(YTDocument document) {
      // TODO Auto-generated method stub

    }
  }

  public class ClassSerializable implements Serializable {

    private String aaa;
  }
}
