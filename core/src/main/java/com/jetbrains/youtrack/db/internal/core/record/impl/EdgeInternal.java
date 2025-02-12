package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public interface EdgeInternal extends Edge, EntityInternal {

  @Nullable
  EntityImpl getBaseEntity();

  @Override
  default boolean isLabeled(String[] labels) {
    if (labels == null) {
      return true;
    }
    if (labels.length == 0) {
      return true;
    }
    Set<String> types = new HashSet<>();

    var typeClass = getSchemaType();
    var baseEntity = getBaseEntity();
    var db = baseEntity.getSession();

    if (typeClass.isPresent()) {
      types.add(typeClass.get().getName(db));
      typeClass.get().getAllSuperClasses().stream().map(schemaClass -> schemaClass.getName(db))
          .forEach(types::add);
    } else {
      types.add(CLASS_NAME);
    }

    for (var s : labels) {
      for (var type : types) {
        if (type.equalsIgnoreCase(s)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  default Collection<String> getPropertyNames() {
    return filterPropertyNames(getPropertyNamesInternal());
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

  @Override
  default <RET> RET getProperty(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getProperty(name);
  }

  @Nullable
  @Override
  default Entity getEntityProperty(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getEntityProperty(name);
  }

  @Nullable
  @Override
  default Blob getBlobProperty(String propertyName) {
    checkPropertyName(propertyName);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getBlobProperty(propertyName);
  }


  static void checkPropertyName(String name) {
    if (name.equals(DIRECTION_OUT) || name.equals(DIRECTION_IN)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  @Override
  default boolean isUnloaded() {
    var baseEntity = getBaseEntity();
    if (baseEntity != null) {
      return baseEntity.isUnloaded();
    }
    return true;
  }

  @Override
  default boolean exists() {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return false;
    }

    return baseEntity.exists();
  }

  @Override
  default void setProperty(String name, Object value) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    baseEntity.setProperty(name, value);
  }

  @Override
  default <T> List<T> getOrCreateEmbeddedList(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateEmbeddedList(name);
  }

  @Override
  default <T> Set<T> getOrCreateEmbeddedSet(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateEmbeddedSet(name);
  }

  @Override
  default <T> Map<String, T> getOrCreateEmbeddedMap(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateEmbeddedMap(name);
  }

  @Override
  default List<Identifiable> getOrCreateLinkList(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateLinkList(name);
  }

  @Override
  default Set<Identifiable> getOrCreateLinkSet(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateLinkSet(name);
  }

  @Override
  default Map<String, Identifiable> getOrCreateLinkMap(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.getOrCreateLinkMap(name);
  }

  @Override
  default boolean hasProperty(final String propertyName) {
    checkPropertyName(propertyName);
    var baseEntity = getBaseEntity();

    if (baseEntity == null) {
      return false;
    }

    return baseEntity.hasProperty(propertyName);
  }

  @Override
  default void setProperty(String name, Object value, PropertyType fieldType) {
    checkPropertyName(name);
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    baseEntity.setProperty(name, value, fieldType);
  }

  @Override
  default <RET> RET removeProperty(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.removeProperty(name);
  }

  @Nullable
  @Override
  default Identifiable getLinkProperty(String name) {
    checkPropertyName(name);

    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getLinkProperty(name);
  }

  @Override
  default Collection<String> getPropertyNamesInternal() {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return Collections.emptySet();
    }
    return baseEntity.getPropertyNamesInternal();
  }

  default <RET> RET getPropertyInternal(String name) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyInternal(name);
  }

  @Override
  default <RET> RET getPropertyInternal(String name, boolean lazyLoading) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyInternal(name, lazyLoading);
  }

  @Override
  default <RET> RET getPropertyOnLoadValue(String name) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getPropertyOnLoadValue(name);
  }

  @Nullable
  @Override
  default Identifiable getLinkPropertyInternal(String name) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      return null;
    }

    return baseEntity.getLinkPropertyInternal(name);
  }

  @Override
  default void setPropertyInternal(String name, Object value) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    baseEntity.setPropertyInternal(name, value);
  }

  @Override
  default void setPropertyInternal(String name, Object value, PropertyType type) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    baseEntity.setPropertyInternal(name, value, type);
  }

  @Override
  default <RET> RET removePropertyInternal(String name) {
    var baseEntity = getBaseEntity();
    if (baseEntity == null) {
      throw new UnsupportedOperationException("This edge is not backed by a entity.");
    }

    return baseEntity.removePropertyInternal(name);
  }

}
