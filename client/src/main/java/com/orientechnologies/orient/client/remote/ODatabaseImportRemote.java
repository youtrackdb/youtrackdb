package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.db.document.YTDatabaseSessionRemote;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseImpExpAbstract;
import com.orientechnologies.orient.core.db.tool.ODatabaseTool;
import com.orientechnologies.orient.core.db.tool.YTDatabaseImportException;
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
