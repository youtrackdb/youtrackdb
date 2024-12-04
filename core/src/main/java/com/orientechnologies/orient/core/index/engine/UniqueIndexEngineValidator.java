package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;

public class UniqueIndexEngineValidator implements IndexEngineValidator<Object, YTRID> {

  /**
   *
   */
  private final OIndexUnique indexUnique;

  /**
   * @param oIndexUnique
   */
  public UniqueIndexEngineValidator(OIndexUnique oIndexUnique) {
    indexUnique = oIndexUnique;
  }

  @Override
  public Object validate(Object key, YTRID oldValue, YTRID newValue) {
    if (oldValue != null) {
      var metadata = indexUnique.getMetadata();
      // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
      if (!oldValue.equals(newValue)) {
        final Boolean mergeSameKey =
            metadata != null ? (Boolean) metadata.get(OIndexInternal.MERGE_KEYS) : Boolean.FALSE;
        if (mergeSameKey == null || !mergeSameKey) {
          throw new YTRecordDuplicatedException(
              String.format(
                  "Cannot index record %s: found duplicated key '%s' in index '%s' previously"
                      + " assigned to the record %s",
                  newValue.getIdentity(), key, indexUnique.getName(), oldValue.getIdentity()),
              indexUnique.getName(),
              oldValue.getIdentity(),
              key);
        }
      } else {
        return IndexEngineValidator.IGNORE;
      }
    }

    if (!newValue.getIdentity().isPersistent()) {
      newValue = newValue.getRecord().getIdentity();
    }
    return newValue.getIdentity();
  }
}
