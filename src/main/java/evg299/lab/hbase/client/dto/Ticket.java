package evg299.lab.hbase.client.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
public class Ticket {
    private UUID uuid;
    private OffsetDateTime dateTime;
    private String ownerId;
    private long deviceId;
    private long numOnDevice;
    private List<TicketItem> items = new ArrayList<>();

    public BigDecimal getSum() {
        BigDecimal sum = BigDecimal.ZERO;
        for (TicketItem item : items) {
            sum = sum.add(item.getPrice().multiply(item.getAmount()));
        }
        return sum;
    }

    private boolean rejected;
    private UUID rejectUuid;
    private OffsetDateTime rejectDateTime;
    private String rejectReason;
}
