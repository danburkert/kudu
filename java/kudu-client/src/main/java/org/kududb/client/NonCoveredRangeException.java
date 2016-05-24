package org.kududb.client;

/**
 * Exception indicating that an operation attempted to access a non-covered range partition.
 */
public class NonCoveredRangeException extends KuduException {
  private final byte[] nonCoveredRangeStart;
  private final byte[] nonCoveredRangeEnd;

  public NonCoveredRangeException(byte[] nonCoveredRangeStart, byte[] nonCoveredRangeEnd) {
    super("non-covered range");
    this.nonCoveredRangeStart = nonCoveredRangeStart;
    this.nonCoveredRangeEnd = nonCoveredRangeEnd;
  }

  byte[] getNonCoveredRangeStart() {
    return nonCoveredRangeStart;
  }

  byte[] getNonCoveredRangeEnd() {
    return nonCoveredRangeEnd;
  }

  @Override
  public String toString() {
    return String.format("NonCoveredRangeException([%s, %s))",
                         Bytes.hex(nonCoveredRangeStart), Bytes.hex(nonCoveredRangeEnd));
  }
}
