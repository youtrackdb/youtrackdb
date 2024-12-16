package com.jetbrains.youtrack.db.internal.core.security;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyAccess;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class PropertyAccessTest extends DbTestBase {

  @Test
  public void testNotAccessible() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    assertEquals("one value", doc.getProperty("name"));
    assertEquals("one value", doc.field("name"));
    assertTrue(doc.containsField("name"));
    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertNull(doc.getProperty("name"));
    assertNull(doc.field("name"));
    assertNull(doc.field("name", PropertyType.STRING));
    assertNull(doc.field("name", String.class));
    assertFalse(doc.containsField("name"));
    assertNull(doc.fieldType("name"));
  }

  @Test
  public void testNotAccessibleAfterConvert() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    EntityImpl doc1 = new EntityImpl();
    RecordInternal.unsetDirty(doc1);
    doc1.fromStream(doc.toStream());
    assertEquals("one value", doc1.getProperty("name"));
    assertEquals("one value", doc1.field("name"));
    assertTrue(doc1.containsField("name"));
    assertEquals(PropertyType.STRING, doc1.fieldType("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc1, new PropertyAccess(toHide));
    assertNull(doc1.getProperty("name"));
    assertNull(doc1.field("name"));
    assertFalse(doc1.containsField("name"));
    assertNull(doc1.fieldType("name"));
  }

  @Test
  public void testNotAccessiblePropertyListing() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    assertArrayEquals(new String[]{"name"}, doc.getPropertyNames().toArray());
    assertArrayEquals(
        new String[]{"one value"},
        doc.getPropertyNames().stream().map(doc::getProperty).toArray());
    assertEquals(new HashSet<String>(List.of("name")), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertEquals("name", e.getKey());
    }

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertArrayEquals(new String[]{}, doc.fieldNames());
    assertArrayEquals(new String[]{}, doc.fieldValues());
    assertEquals(new HashSet<String>(), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertNotEquals("name", e.getKey());
    }
  }

  @Test
  public void testNotAccessiblePropertyListingSer() {
    EntityImpl docPre = new EntityImpl();
    docPre.setProperty("name", "one value");
    assertArrayEquals(new String[]{"name"}, docPre.getPropertyNames().toArray());
    assertArrayEquals(
        new String[]{"one value"},
        docPre.getPropertyNames().stream().map(docPre::getProperty).toArray());
    assertEquals(new HashSet<String>(List.of("name")), docPre.getPropertyNames());
    for (Map.Entry<String, Object> e : docPre) {
      assertEquals("name", e.getKey());
    }

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityImpl doc = new EntityImpl();
    RecordInternal.unsetDirty(doc);
    doc.fromStream(docPre.toStream());
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertArrayEquals(new String[]{}, doc.getPropertyNames().toArray());
    assertArrayEquals(
        new String[]{}, doc.getPropertyNames().stream().map(doc::getProperty).toArray());
    assertEquals(new HashSet<String>(), doc.getPropertyNames());
    for (Map.Entry<String, Object> e : doc) {
      assertNotEquals("name", e.getKey());
    }
  }

  @Test
  public void testJsonSerialization() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    assertTrue(doc.toJSON().contains("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertFalse(doc.toJSON().contains("name"));
  }

  @Test
  public void testToMap() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    assertTrue(doc.toMap().containsKey("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertFalse(doc.toMap().containsKey("name"));
  }

  @Test
  public void testStringSerialization() {
    EntityImpl doc = new EntityImpl();
    doc.setProperty("name", "one value");
    assertTrue(doc.toString().contains("name"));

    Set<String> toHide = new HashSet<>();
    toHide.add("name");
    EntityInternalUtils.setPropertyAccess(doc, new PropertyAccess(toHide));
    assertFalse(doc.toString().contains("name"));
  }
}
