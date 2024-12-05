package com.orientechnologies.core.index.multivalue;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.index.OIndexKeyUpdater;
import com.orientechnologies.core.index.OIndexUpdateAction;
import com.orientechnologies.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
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
