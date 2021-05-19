package io.raven.vertx.ranger.common;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {

  public static final int MINIMUM_REFRESH_TIME = 5000;
  public static final String ALL_ENV = "*";

  public static final String DEFAULT_NAMESPACE = "default";
  public static final String DEFAULT_HOST = "__DEFAULT_SERVICE_HOST";
  public static final int DEFAULT_PORT = -1;
  public static final int DEFAULT_CHECK_INTERVAl = 15;
  public static final int DEFAULT_RETRY_CONN_INTERVAL = 5000;
}
