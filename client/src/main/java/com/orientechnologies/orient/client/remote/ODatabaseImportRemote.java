package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.command.OCommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseImpExpAbstract;
import com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseTool;
import com.jetbrains.youtrack.db.internal.core.db.tool.YTDatabaseImportException;
import com.orientechnologies.orient.client.remote.db.document.YTDatabaseSessionRemote;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 *
 */
public class ODatabaseImportRemote extends ODatabaseImpExpAbstract {

  private String options;

  public ODatabaseImportRemote(
      YTDatabaseSessionInternal iDatabase, String iFileName, OCommandOutputListener iListener) {
    super(iDatabase, iFileName, iListener);
  }

  @Override
  public void run() {
    try {
      importDatabase();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during database import", e);
    }
  }

  @Override
  public ODatabaseTool setOptions(String iOptions) {
    this.options = iOptions;
    return super.setOptions(iOptions);
  }

  public void importDatabase() throws YTDatabaseImportException {
    OStorageRemote storage = (OStorageRemote) getDatabase().getStorage();
    File file = new File(getFileName());
    try {
      storage.importDatabase((YTDatabaseSessionRemote) database, options, new FileInputStream(file),
          file.getName(),
          getListener());
    } catch (FileNotFoundException e) {
      throw YTException.wrapException(
          new YTDatabaseImportException("Error importing the database"), e);
    }
  }

  public void close() {
  }
}
