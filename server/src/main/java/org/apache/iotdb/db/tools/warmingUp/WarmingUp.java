/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.tools.warmingUp;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarmingUp {

  private static final Logger LOGGER = LoggerFactory.getLogger(WarmingUp.class);

  private static final WarmingUp INSTACNE = new WarmingUp();
  public static final String JDBC_DRIVER_NAME = "org.apache.iotdb.jdbc.IoTDBDriver";
  private static IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
  private static ExecutorService warmingUpThreadPool;
  private static Connection connection;

  static {
    try {
      warmingUpThreadPool = Executors.newFixedThreadPool(1, r -> new Thread(r));
      Class.forName(Config.JDBC_DRIVER_NAME);
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:" + ioTDBConfig.getRpcPort() + "/",
              "root", "root");
    } catch (Exception e) {
      LOGGER.error("cannot establish connection with IoTDB", e);
    }
  }

  public static WarmingUp getInstance() {
    return INSTACNE;
  }

  public void submitWarming() {
    warmingUpThreadPool.submit(new WarmingUpThread());
  }

  class WarmingUpThread implements Runnable {

    @Override
    public void run() {
      try (Statement statement = connection.createStatement()) {
        ResultSet resultSet = null;
        boolean hasTimeseries;
        boolean hasResultSet;

        hasTimeseries = statement.execute("count timeseries root");
        if (hasTimeseries) {
          resultSet = statement.getResultSet();
          if (resultSet.next()) {
            hasTimeseries = !resultSet.getString(1).equals("0");
          }
        }
        if (hasTimeseries) {
          if (ioTDBConfig.getWarmingUp().equals("first")) {
            hasResultSet = statement.execute("select first(*) from root group by device");
          } else {
            hasResultSet = statement.execute("select last(*) from root group by device");
          }
          if (hasResultSet) {
            resultSet = statement.getResultSet();
            while (resultSet.next()) {
              // pass
            }
          }
        }
        LOGGER.info("warming up finished");
      } catch (Exception e) {
        LOGGER.warn("warming up failed", e.getMessage());
      } finally {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            LOGGER.error("close connection error during warming up operation", e);
          }
        }
      }
    }
  }

}
