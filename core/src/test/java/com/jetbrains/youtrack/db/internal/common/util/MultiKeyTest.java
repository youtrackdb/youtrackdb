package com.jetbrains.youtrack.db.internal.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class MultiKeyTest {

  @Test
  public void testEqualsDifferentSize() {
    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    final MultiKey anotherMultiKey = new MultiKey(Arrays.asList("a", "b"));

    assertNotEquals(multiKey, anotherMultiKey);
  }

  @Test
  public void testEqualsDifferentItems() {
    final MultiKey multiKey = new MultiKey(Arrays.asList("b", "c"));
    final MultiKey anotherMultiKey = new MultiKey(Arrays.asList("a", "b"));

    assertNotEquals(multiKey, anotherMultiKey);
  }

  @Test
  public void testEqualsTheSame() {
    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    assertEquals(multiKey, multiKey);
  }

  @Test
  public void testEqualsNull() {
    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    assertNotEquals(null, multiKey);
  }

  @Test
  public void testEqualsDifferentClass() {
    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    assertNotEquals("a", multiKey);
  }

  @Test
  public void testEmptyKeyEquals() {
    final Map<MultiKey, Object> multiKeyMap = new HashMap<MultiKey, Object>();

    final MultiKey multiKey = new MultiKey(Collections.emptyList());
    multiKeyMap.put(multiKey, new Object());

    final MultiKey anotherMultiKey = new MultiKey(Collections.emptyList());
    final Object mapResult = multiKeyMap.get(anotherMultiKey);

    assertNotNull(mapResult);
  }

  @Test
  public void testOneKeyMap() {
    final Map<MultiKey, Object> multiKeyMap = new HashMap<MultiKey, Object>();

    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    multiKeyMap.put(multiKey, new Object());

    final MultiKey anotherMultiKey = new MultiKey(Collections.singletonList("a"));
    final Object mapResult = multiKeyMap.get(anotherMultiKey);

    assertNotNull(mapResult);
  }

  @Test
  public void testOneKeyNotInMap() {
    final Map<MultiKey, Object> multiKeyMap = new HashMap<MultiKey, Object>();

    final MultiKey multiKey = new MultiKey(Collections.singletonList("a"));
    multiKeyMap.put(multiKey, new Object());

    final MultiKey anotherMultiKey = new MultiKey(Collections.singletonList("b"));
    final Object mapResult = multiKeyMap.get(anotherMultiKey);

    assertNull(mapResult);
  }

  @Test
  public void testTwoKeyMap() {
    final Map<MultiKey, Object> multiKeyMap = new HashMap<MultiKey, Object>();

    final MultiKey multiKey = new MultiKey(Arrays.asList("a", "b"));
    multiKeyMap.put(multiKey, new Object());

    final MultiKey anotherMultiKey = new MultiKey(Arrays.asList("a", "b"));
    final Object mapResult = multiKeyMap.get(anotherMultiKey);

    assertNotNull(mapResult);
  }

  @Test
  public void testTwoKeyMapReordered() {
    final Map<MultiKey, Object> multiKeyMap = new HashMap<MultiKey, Object>();

    final MultiKey multiKey = new MultiKey(Arrays.asList("a", "b"));
    multiKeyMap.put(multiKey, new Object());

    final MultiKey anotherMultiKey = new MultiKey(Arrays.asList("b", "a"));
    final Object mapResult = multiKeyMap.get(anotherMultiKey);

    assertNotNull(mapResult);
  }
}
