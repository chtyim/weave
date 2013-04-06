package com.continuuity.internal.kafka.client;

/**
*
*/
public enum Compression {
  NONE(0),
  GZIP(1),
  SNAPPY(2);

  private final int code;

  Compression(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static Compression fromCode(int code) {
    switch (code) {
      case 0:
        return NONE;
      case 1:
        return GZIP;
      case 2:
        return SNAPPY;
    }
    throw new IllegalArgumentException("Unknown compression code.");
  }
}
