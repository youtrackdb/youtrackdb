package com.jetbrains.youtrack.db.internal.core.db.record.ridbag;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 *
 */
public final class RidBagDeleter {

  public static void deleteAllRidBags(EntityImpl document) {
    for (var propertyName : document.getPropertyNamesInternal()) {
      var value = document.getPropertyInternal(propertyName);
      if (value instanceof RidBag) {
        ((RidBag) value).delete();
      }
    }
  }
}
