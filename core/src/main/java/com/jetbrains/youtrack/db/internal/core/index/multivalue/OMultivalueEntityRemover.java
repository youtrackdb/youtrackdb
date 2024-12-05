package com.jetbrains.youtrack.db.internal.core.index.multivalue;

import com.jetbrains.youtrack.db.internal.common.types.OModifiableBoolean;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.index.OIndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.OIndexUpdateAction;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OMultivalueEntityRemover implements OIndexKeyUpdater<Object> {

  private final YTIdentifiable value;
  private final OModifiableBoolean removed;

  public OMultivalueEntityRemover(YTIdentifiable value, OModifiableBoolean removed) {
    this.value = value;
    this.removed = removed;
  }

  @Override
  public OIndexUpdateAction<Object> update(Object persistentValue, AtomicLong bonsayFileId) {
    @SuppressWarnings("unchecked")
    Set<YTIdentifiable> values = (Set<YTIdentifiable>) persistentValue;
    if (value == null) {
      removed.setValue(true);

      //noinspection unchecked
      return OIndexUpdateAction.remove();
    } else if (values.remove(value)) {
      removed.setValue(true);

      if (values.isEmpty()) {
        // remove tree ridbag too
        if (values instanceof OMixedIndexRIDContainer) {
          ((OMixedIndexRIDContainer) values).delete();
        } else if (values instanceof OIndexRIDContainerSBTree) {
          ((OIndexRIDContainerSBTree) values).delete();
        }

        //noinspection unchecked
        return OIndexUpdateAction.remove();
      } else {
        return OIndexUpdateAction.changed(values);
      }
    }

    return OIndexUpdateAction.changed(values);
  }
}
