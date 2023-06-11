package evg299.lab.hbase.client.dao.tables;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TicketTable {
    public static final String TABLE_NAME = "tickets";

    public static final byte[] IDX_COLUMN_FAMILY = "fields_for_search".getBytes(StandardCharsets.UTF_8);
    public static final byte[] MAIN_COLUMN_FAMILY = "serialized_all_data".getBytes(StandardCharsets.UTF_8);

    // IDX_COLUMN_FAMILY
    public static final byte[] DT_COLUMN = "date_time".getBytes(StandardCharsets.UTF_8);
    public static final byte[] OWNER_COLUMN = "owner_id".getBytes(StandardCharsets.UTF_8);
    public static final byte[] DEVICE_COLUMN = "device_id".getBytes(StandardCharsets.UTF_8);
    public static final byte[] SUM_COLUMN = "sum".getBytes(StandardCharsets.UTF_8);
    public static final byte[] REJECTED_COLUMN = "rejected".getBytes(StandardCharsets.UTF_8);

    // MAIN_COLUMN_FAMILY
    public static final byte[] SRC_COLUMN = "ticket_json".getBytes(StandardCharsets.UTF_8);
}
