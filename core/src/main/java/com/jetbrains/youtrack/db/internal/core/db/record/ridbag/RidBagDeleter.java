package com.jetbrains.youtrack.db.internal.core.db.record.ridbag;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

/**
 *
 */
public final class RidBagDeleter {

  public static void deleteAllRidBags(EntityImpl entity) {
    for (var propertyName : entity.getPropertyNamesInternal()) {
      var value = entity.getPropertyInternal(propertyName);
      if (value instanceof RidBag) {
        ((RidBag) value).delete();
      }
    }
  }
}
