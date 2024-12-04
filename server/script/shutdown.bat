@echo off
rem
rem Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
rem
rem HISTORY:
rem 2012-07-31: Added -w option
rem
rem

rem Guess YOU_TRACK_DB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA=java
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%YOU_TRACK_DB_HOME%" == "" goto gotHome
set YOU_TRACK_DB_HOME=%CURRENT_DIR%
if exist "%YOU_TRACK_DB_HOME%\bin\server.bat" goto okHome
cd ..
set YOU_TRACK_DB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%YOU_TRACK_DB_HOME%\bin\server.bat" goto okHome
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
if NOT exist "%CONFIG_FILE%" set CONFIG_FILE=%YOU_TRACK_DB_HOME%/config/orientdb-server-config.xml

set LOG_FILE=%YOU_TRACK_DB_HOME%/config/orientdb-server-log.properties
set LOG_LEVEL=warning
set WWW_PATH=%YOU_TRACK_DB_HOME%/www
set JAVA_OPTS=-Djava.awt.headless=true

call %JAVA% -client %JAVA_OPTS% -Dyoutrackdb.config.file="%CONFIG_FILE%" -cp "%YOU_TRACK_DB_HOME%\lib\orientdb-tools-@VERSION@.jar;%YOU_TRACK_DB_HOME%\lib\*" com.orientechnologies.orient.server.OServerShutdownMain %CMD_LINE_ARGS%

:end
