package com.jetbrains.youtrack.db.internal.core.index.multivalue;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.DefaultIndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.IndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.IndexRIDContainer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.MixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public final class MultivalueIndexKeyUpdaterImpl implements IndexKeyUpdater<Object> {

  private final RID identity;
  private final String indexName;
  private final boolean mixedContainer;

  public MultivalueIndexKeyUpdaterImpl(
      RID identity, String valueContainerAlgorithm, int binaryFormatVersion, String indexName) {
    this.identity = identity;
    this.indexName = indexName;
    if (DefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
      mixedContainer = binaryFormatVersion >= 13;
    } else {
      throw new IllegalStateException("MVRBTree is not supported any more");
    }
  }

  @Override
  public IndexUpdateAction<Object> update(Object oldValue, AtomicLong bonsayFileId) {
    Set<Identifiable> toUpdate = (Set<Identifiable>) oldValue;
    if (toUpdate == null) {
      if (mixedContainer) {
        toUpdate = new MixedIndexRIDContainer(indexName, bonsayFileId);
      } else {
        toUpdate = new IndexRIDContainer(indexName, true, bonsayFileId);
      }
    }
    if (toUpdate instanceof IndexRIDContainer) {
      boolean isTree = !((IndexRIDContainer) toUpdate).isEmbedded();
      toUpdate.add(identity);

      if (isTree) {
        //noinspection unchecked
        return IndexUpdateAction.nothing();
      } else {
        return IndexUpdateAction.changed(toUpdate);
      }
    } else if (toUpdate instanceof MixedIndexRIDContainer ridContainer) {
      final boolean embeddedWasUpdated = ridContainer.addEntry(identity);

      if (!embeddedWasUpdated) {
        //noinspection unchecked
        return IndexUpdateAction.nothing();
      } else {
        return IndexUpdateAction.changed(toUpdate);
      }
    } else {
      if (toUpdate.add(identity)) {
        return IndexUpdateAction.changed(toUpdate);
      } else {
        //noinspection unchecked
        return IndexUpdateAction.nothing();
      }
    }
  }
}
