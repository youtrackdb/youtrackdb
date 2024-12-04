@echo off
rem
rem Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
rem
rem Guess YOU_TRACK_DB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA="java"
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%YOU_TRACK_DB_HOME%" == "" goto gotHome
set YOU_TRACK_DB_HOME=%CURRENT_DIR%
if exist "%YOU_TRACK_DB_HOME%\bin\console.bat" goto okHome
cd ..
set YOU_TRACK_DB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%YOU_TRACK_DB_HOME%\bin\console.bat" goto okHome
echo The YOU_TRACK_DB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
rem Get remaining unshifted command line arguments and save them in the
set CMD_LINE_ARGS=

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs

set KEYSTORE=%YOU_TRACK_DB_HOME%\config\cert\orientdb-console.ks
set KEYSTORE_PASS=password
set TRUSTSTORE=%YOU_TRACK_DB_HOME%\config\cert\orientdb-console.ts
set TRUSTSTORE_PASS=password
set SSL_OPTS="-Dclient.ssl.enabled=false -Djavax.net.ssl.keyStore=%KEYSTORE% -Djavax.net.ssl.keyStorePassword=%KEYSTORE_PASS% -Djavax.net.ssl.trustStore=%TRUSTSTORE% -Djavax.net.ssl.trustStorePassword=%TRUSTSTORE_PASS%"

set ORIENTDB_SETTINGS=-Xmx1024m -Djna.nosys=true -Djava.util.logging.config.file="%YOU_TRACK_DB_HOME%\config\orientdb-client-log.properties" -Djava.awt.headless=true

call %JAVA% -client %SSL_OPTS% %ORIENTDB_SETTINGS% -Dfile.encoding=utf-8 -Dorientdb.build.number="@BUILD@" -cp "%YOU_TRACK_DB_HOME%\lib\*;%YOU_TRACK_DB_HOME%\plugins\*" com.orientechnologies.orient.console.OConsoleDatabaseApp %CMD_LINE_ARGS%

:end
