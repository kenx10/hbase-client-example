package evg299.lab.hbase.client.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TicketItem {
    private String name;
    private String code;
    private BigDecimal price;
    private BigDecimal amount;
}
