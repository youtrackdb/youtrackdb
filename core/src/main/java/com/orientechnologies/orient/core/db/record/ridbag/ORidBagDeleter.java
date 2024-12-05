package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.orient.core.record.impl.YTEntityImpl;

/**
 *
 */
public final class ORidBagDeleter {

  public static void deleteAllRidBags(YTEntityImpl document) {
    for (var propertyName : document.getPropertyNamesInternal()) {
      var value = document.getPropertyInternal(propertyName);
      if (value instanceof RidBag) {
        ((RidBag) value).delete();
      }
    }
  }
}
