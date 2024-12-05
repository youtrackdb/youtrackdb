package com.jetbrains.youtrack.db.internal.core.index.multivalue;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.ODefaultIndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.OIndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.OIndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public final class OMultivalueIndexKeyUpdaterImpl implements OIndexKeyUpdater<Object> {

  private final YTRID identity;
  private final String indexName;
  private final boolean mixedContainer;

  public OMultivalueIndexKeyUpdaterImpl(
      YTRID identity, String valueContainerAlgorithm, int binaryFormatVersion, String indexName) {
    this.identity = identity;
    this.indexName = indexName;
    if (ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER.equals(valueContainerAlgorithm)) {
      mixedContainer = binaryFormatVersion >= 13;
    } else {
      throw new IllegalStateException("MVRBTree is not supported any more");
    }
  }

  @Override
  public OIndexUpdateAction<Object> update(Object oldValue, AtomicLong bonsayFileId) {
    Set<YTIdentifiable> toUpdate = (Set<YTIdentifiable>) oldValue;
    if (toUpdate == null) {
      if (mixedContainer) {
        toUpdate = new OMixedIndexRIDContainer(indexName, bonsayFileId);
      } else {
        toUpdate = new OIndexRIDContainer(indexName, true, bonsayFileId);
      }
    }
    if (toUpdate instanceof OIndexRIDContainer) {
      boolean isTree = !((OIndexRIDContainer) toUpdate).isEmbedded();
      toUpdate.add(identity);

      if (isTree) {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      } else {
        return OIndexUpdateAction.changed(toUpdate);
      }
    } else if (toUpdate instanceof OMixedIndexRIDContainer ridContainer) {
      final boolean embeddedWasUpdated = ridContainer.addEntry(identity);

      if (!embeddedWasUpdated) {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      } else {
        return OIndexUpdateAction.changed(toUpdate);
      }
    } else {
      if (toUpdate.add(identity)) {
        return OIndexUpdateAction.changed(toUpdate);
      } else {
        //noinspection unchecked
        return OIndexUpdateAction.nothing();
      }
    }
  }
}
