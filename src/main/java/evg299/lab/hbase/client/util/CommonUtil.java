package evg299.lab.hbase.client.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtil {

    public static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    public static byte[] offsetDateTimeToBytes(OffsetDateTime offsetDateTime) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
        bb.putLong(offsetDateTime.toInstant().getEpochSecond());
        return bb.array();
    }

    public static byte[] stringToBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] longToBytes(long l) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[8]);
        bb.putLong(l);
        return bb.array();
    }

    public static byte[] booleanToBytes(boolean b) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
        bb.put((byte) (b ? 1 : 0));
        return bb.array();
    }
}
