package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

public interface EdgeInternal extends Edge, EntityInternal {

  @Nullable
  EntityImpl getBaseDocument();

  @Override
  default Set<String> getPropertyNames() {
    return filterPropertyNames(getPropertyNamesInternal());
  }

  static Set<String> filterPropertyNames(Set<String> propertyNames) {
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

  @Override
  default <RET> RET getProperty(String name) {
    checkPropertyName(name);

    return getPropertyInternal(name);
  }

  static void checkPropertyName(String name) {
    if (name.equals(DIRECTION_OUT) || name.equals(DIRECTION_IN)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  @Override
  default boolean isUnloaded() {
    var baseEntity = getBaseDocument();
    if (baseEntity != null) {
      return baseEntity.isUnloaded();
    }
    return true;
  }

  @Override
  default boolean exists() {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return false;
    }

    return baseEntity.exists();
  }

  @Override
  default void setProperty(String name, Object value) {
    checkPropertyName(name);

    setPropertyInternal(name, value);
  }

  @Override
  default boolean hasProperty(final String propertyName) {
    checkPropertyName(propertyName);
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return false;
    }

    return baseEntity.hasProperty(propertyName);
  }

  @Override
  default void setProperty(String name, Object value, PropertyType... fieldType) {
    checkPropertyName(name);
    setPropertyInternal(name, value, fieldType);
  }

  @Override
  default <RET> RET removeProperty(String name) {
    checkPropertyName(name);

    return removePropertyInternal(name);
  }

  @Nullable
  @Override
  default Identifiable getLinkProperty(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getLinkProperty(name);
  }

  @Override
  default Set<String> getPropertyNamesInternal() {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return Collections.emptySet();
    }
    return baseEntity.getPropertyNamesInternal();
  }

  default <RET> RET getPropertyInternal(String name) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyInternal(name);
  }

  @Override
  default <RET> RET getPropertyInternal(String name, boolean lazyLoading) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyInternal(name, lazyLoading);
  }

  @Override
  default <RET> RET getPropertyOnLoadValue(String name) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyOnLoadValue(name);
  }

  @Nullable
  @Override
  default Identifiable getLinkPropertyInternal(String name) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getLinkPropertyInternal(name);
  }

  @Override
  default void setPropertyInternal(String name, Object value) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      promoteToRegularEdge();
      baseEntity = getBaseDocument();

      if (baseEntity == null) {
        throw new UnsupportedOperationException("This edge is not backed by a entity.");
      }
    }

    baseEntity.setPropertyInternal(name, value);
  }

  @Override
  default void setPropertyInternal(String name, Object value, PropertyType... type) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      promoteToRegularEdge();

      baseEntity = getBaseDocument();
      if (baseEntity == null) {
        throw new UnsupportedOperationException("This edge is not backed by a entity.");
      }
    }

    baseEntity.setPropertyInternal(name, value, type);
  }

  @Override
  default <RET> RET removePropertyInternal(String name) {
    var baseEntity = getBaseDocument();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.removePropertyInternal(name);
  }

  void promoteToRegularEdge();
}
