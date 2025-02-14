package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Edge;
import java.util.ArrayList;
import java.util.Collection;

public interface EdgeInternal extends Edge, EntityInternal {

  static void checkPropertyName(String name) {
    if (name.equals(DIRECTION_OUT) || name.equals(DIRECTION_IN)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  static Collection<String> filterPropertyNames(Collection<String> propertyNames) {
    var propertiesToRemove = new ArrayList<String>();

    for (var propertyName : propertyNames) {
      if (propertyName.equals(DIRECTION_IN) || propertyName.equals(DIRECTION_OUT)) {
        propertiesToRemove.add(propertyName);
      }
    }

    if (propertiesToRemove.isEmpty()) {
      return propertyNames;
    }

    for (var propertyToRemove : propertiesToRemove) {
      propertyNames.remove(propertyToRemove);
    }

    return propertyNames;
  }
}
