@echo off
title billboard-service
set path=jdk-15.0.1+9-jre\bin;%path%
set JAVA_HOME=jdk-15.0.1+9-jre
cd service
@echo on
bin\billboard.service.202012-1.bat
