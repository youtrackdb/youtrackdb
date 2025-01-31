package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSecurityResourceSegment;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SecurityResource {

  private static final Map<String, SecurityResource> cache = new ConcurrentHashMap<>();

  public static SecurityResource getInstance(String resource) {
    var result = cache.get(resource);
    if (result == null) {
      result = parseResource(resource);
      if (result != null) {
        cache.put(resource, result);
      }
    }
    return result;
  }

  protected static SecurityResource parseResource(String resource) {

    if (resource.equals("*")) {
      return SecurityResourceAll.INSTANCE;
    } else if (resource.equals("database.schema")) {
      return SecurityResourceSchema.INSTANCE;
    } else if (resource.equals("database.class.*")) {
      return SecurityResourceClass.ALL_CLASSES;
    } else if (resource.equals("database.class.*.*")) {
      return SecurityResourceProperty.ALL_PROPERTIES;
    } else if (resource.equals("database.cluster.*")) {
      return SecurityResourceCluster.ALL_CLUSTERS;
    } else if (resource.equals("database.systemclusters")) {
      return SecurityResourceCluster.SYSTEM_CLUSTERS;
    } else if (resource.equals("database.function.*")) {
      return SecurityResourceFunction.ALL_FUNCTIONS;
    } else if (resource.equals("database")) {
      return SecurityResourceDatabaseOp.DB;
    } else if (resource.equals("database.create")) {
      return SecurityResourceDatabaseOp.CREATE;
    } else if (resource.equals("database.copy")) {
      return SecurityResourceDatabaseOp.COPY;
    } else if (resource.equals("database.drop")) {
      return SecurityResourceDatabaseOp.DROP;
    } else if (resource.equals("database.exists")) {
      return SecurityResourceDatabaseOp.EXISTS;
    } else if (resource.equals("database.command")) {
      return SecurityResourceDatabaseOp.COMMAND;
    } else if (resource.equals("database.command.gremlin")) {
      return SecurityResourceDatabaseOp.COMMAND_GREMLIN;
    } else if (resource.equals("database.freeze")) {
      return SecurityResourceDatabaseOp.FREEZE;
    } else if (resource.equals("database.release")) {
      return SecurityResourceDatabaseOp.RELEASE;
    } else if (resource.equals("database.passthrough")) {
      return SecurityResourceDatabaseOp.PASS_THROUGH;
    } else if (resource.equals("database.bypassRestricted")) {
      return SecurityResourceDatabaseOp.BYPASS_RESTRICTED;
    } else if (resource.equals("database.hook.record")) {
      return SecurityResourceDatabaseOp.HOOK_RECORD;
    } else if (resource.equals("server")) {
      return SecurityResourceServerOp.SERVER;
    } else if (resource.equals("server.status")) {
      return SecurityResourceServerOp.STATUS;
    } else if (resource.equals("server.remove")) {
      return SecurityResourceServerOp.REMOVE;
    } else if (resource.equals("server.admin")) {
      return SecurityResourceServerOp.ADMIN;
    }
    try {
      var parsed = SQLEngine.parseSecurityResource(resource);

      if (resource.startsWith("database.class.")) {
        var classElement = parsed.getNext().getNext();
        String className;
        var allClasses = false;
        if (classElement.getIdentifier() != null) {
          className = classElement.getIdentifier().getStringValue();
        } else {
          className = null;
          allClasses = true;
        }
        var propertyModifier = classElement.getNext();
        if (propertyModifier != null) {
          if (propertyModifier.getNext() != null) {
            throw new SecurityException("Invalid resource: " + resource);
          }
          var propertyName = propertyModifier.getIdentifier().getStringValue();
          if (allClasses) {
            return new SecurityResourceProperty(resource, propertyName);
          } else {
            return new SecurityResourceProperty(resource, className, propertyName);
          }
        } else {
          return new SecurityResourceClass(resource, className);
        }
      } else if (resource.startsWith("database.cluster.")) {
        var clusterElement = parsed.getNext().getNext();
        var clusterName = clusterElement.getIdentifier().getStringValue();
        if (clusterElement.getNext() != null) {
          throw new SecurityException("Invalid resource: " + resource);
        }
        return new SecurityResourceCluster(resource, clusterName);
      } else if (resource.startsWith("database.function.")) {
        var functionElement = parsed.getNext().getNext();
        var functionName = functionElement.getIdentifier().getStringValue();
        if (functionElement.getNext() != null) {
          throw new SecurityException("Invalid resource: " + resource);
        }
        return new SecurityResourceFunction(resource, functionName);
      } else if (resource.startsWith("database.systemclusters.")) {
        var clusterElement = parsed.getNext().getNext();
        var clusterName = clusterElement.getIdentifier().getStringValue();
        if (clusterElement.getNext() != null) {
          throw new SecurityException("Invalid resource: " + resource);
        }
        return new SecurityResourceCluster(resource, clusterName);
      }

      throw new SecurityException("Invalid resource: " + resource);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SecurityException("Invalid resource: " + resource);
    }
  }

  protected final String resourceString;

  public SecurityResource(String resourceString) {
    this.resourceString = resourceString;
  }

  public String getResourceString() {
    return resourceString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var that = (SecurityResource) o;
    return Objects.equals(resourceString, that.resourceString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceString);
  }
}
