package io.confluent.connect.Utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeUtil {
    public static String encodeTimestamp(long partitionDurationMs, String pathFormat, String timeZoneString, long timestamp) {
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pathFormat).withZone(timeZone);
        DateTime partition = new DateTime(getPartition(partitionDurationMs, timestamp, timeZone));
        return partition.toString(formatter);
    }

    private static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {
        long adjustedTimeStamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
        return timeZone.convertLocalToUTC(partitionedTime, false);
    }
}