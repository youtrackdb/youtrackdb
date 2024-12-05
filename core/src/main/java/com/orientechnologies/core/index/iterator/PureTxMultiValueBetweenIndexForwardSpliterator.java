package com.orientechnologies.core.index.iterator;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndexMultiValues;
import com.orientechnologies.core.iterator.OEmptyIterator;
import com.orientechnologies.core.tx.OTransactionIndexChanges;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PureTxMultiValueBetweenIndexForwardSpliterator
    implements Spliterator<ORawPair<Object, YTRID>> {

  /**
   *
   */
  private final OIndexMultiValues index;

  private final OTransactionIndexChanges indexChanges;
  private Object lastKey;

  private Object nextKey;

  private Iterator<YTIdentifiable> valuesIterator = new OEmptyIterator<>();
  private Object key;

  public PureTxMultiValueBetweenIndexForwardSpliterator(
      OIndexMultiValues index,
      Object fromKey,
      boolean fromInclusive,
      Object toKey,
      boolean toInclusive,
      OTransactionIndexChanges indexChanges) {
    this.index = index;
    this.indexChanges = indexChanges;

    if (fromKey != null) {
      fromKey = this.index.enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
    }
    if (toKey != null) {
      toKey = this.index.enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);
    }

    final Object[] keys = indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
    if (keys.length == 0) {
      nextKey = null;
    } else {
      Object firstKey = keys[0];
      lastKey = keys[1];

      nextKey = firstKey;
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, YTRID>> action) {
    if (valuesIterator.hasNext()) {
      final ORawPair<Object, YTRID> entry = nextEntryInternal();
      action.accept(entry);
      return true;
    }

    if (nextKey == null) {
      return false;
    }

    Set<YTIdentifiable> result;
    do {
      result = OIndexMultiValues.calculateTxValue(nextKey, indexChanges);
      key = nextKey;

      nextKey = indexChanges.getHigherKey(nextKey);

      if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0) {
        nextKey = null;
      }
    } while ((result == null || result.isEmpty()) && nextKey != null);

    if (result == null || result.isEmpty()) {
      return false;
    }

    valuesIterator = result.iterator();
    final ORawPair<Object, YTRID> entry = nextEntryInternal();
    action.accept(entry);

    return true;
  }

  private ORawPair<Object, YTRID> nextEntryInternal() {
    final YTIdentifiable identifiable = valuesIterator.next();
    return new ORawPair<>(key, identifiable.getIdentity());
  }

  @Override
  public Spliterator<ORawPair<Object, YTRID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return NONNULL | ORDERED | SORTED;
  }

  @Override
  public Comparator<? super ORawPair<Object, YTRID>> getComparator() {
    return (entryOne, entryTwo) ->
        ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
  }
}
