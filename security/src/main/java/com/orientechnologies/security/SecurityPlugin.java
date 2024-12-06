/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.orientechnologies.security;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.ServerPluginAbstract;
import com.orientechnologies.security.auditing.DefaultAuditing;
import com.orientechnologies.security.kerberos.KerberosAuthenticator;
import com.orientechnologies.security.ldap.OLDAPImporter;
import com.orientechnologies.security.password.DefaultPasswordValidator;

public class SecurityPlugin extends ServerPluginAbstract {

  private OServer server;

  @Override
  public void config(OServer server, OServerParameterConfiguration[] iParams) {
    this.server = server;
  }

  @Override
  public String getName() {
    return "security-plugin";
  }

  @Override
  public void startup() {
    registerSecurityComponents();
  }

  @Override
  public void shutdown() {
    unregisterSecurityComponents();
  }

  // The OSecurityModule resides in the main application's class loader. Its configuration file
  // may reference components that are reside in pluggable modules.
  // A security plugin should register its components so that SecuritySystem has access to them.
  private void registerSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server.getSecurity().registerSecurityClass(DefaultAuditing.class);
        server.getSecurity().registerSecurityClass(DefaultPasswordValidator.class);
        server.getSecurity().registerSecurityClass(KerberosAuthenticator.class);
        server.getSecurity().registerSecurityClass(OLDAPImporter.class);
      }
    } catch (Throwable th) {
      LogManager.instance().error(this, "registerSecurityComponents() ", th);
    }
  }

  private void unregisterSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server.getSecurity().unregisterSecurityClass(DefaultAuditing.class);
        server.getSecurity().unregisterSecurityClass(DefaultPasswordValidator.class);
        server.getSecurity().unregisterSecurityClass(KerberosAuthenticator.class);
        server.getSecurity().unregisterSecurityClass(OLDAPImporter.class);
      }
    } catch (Throwable th) {
      LogManager.instance().error(this, "unregisterSecurityComponents()", th);
    }
  }
}
