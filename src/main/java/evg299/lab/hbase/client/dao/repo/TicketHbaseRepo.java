package evg299.lab.hbase.client.dao.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import evg299.lab.hbase.client.dao.api.TicketDao;
import evg299.lab.hbase.client.dao.tables.TicketTable;
import evg299.lab.hbase.client.dto.Ticket;
import evg299.lab.hbase.client.util.CommonUtil;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class TicketHbaseRepo extends TicketTable implements TicketDao {
    private final Connection connection;
    private final ObjectMapper mapper;

    public void createTable() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(IDX_COLUMN_FAMILY).build())
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(MAIN_COLUMN_FAMILY).build())
                    .build();

            admin.createTable(descriptor);
        }
    }

    @Override
    public void deleteById(UUID id) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Delete delete = new Delete(CommonUtil.uuidToBytes(id));
            table.delete(delete);
        }
    }

    @Override
    public void save(Ticket ticket) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Put put = new Put(CommonUtil.uuidToBytes(ticket.getUuid()));
            put.addColumn(IDX_COLUMN_FAMILY, DT_COLUMN, CommonUtil.offsetDateTimeToBytes(ticket.getDateTime()));
            put.addColumn(IDX_COLUMN_FAMILY, OWNER_COLUMN, CommonUtil.stringToBytes(ticket.getOwnerId()));
            put.addColumn(IDX_COLUMN_FAMILY, DEVICE_COLUMN, CommonUtil.longToBytes(ticket.getDeviceId()));
            put.addColumn(IDX_COLUMN_FAMILY, SUM_COLUMN, Bytes.toBytes(ticket.getSum()));
            put.addColumn(IDX_COLUMN_FAMILY, REJECTED_COLUMN, CommonUtil.booleanToBytes(ticket.isRejected()));
            put.addColumn(MAIN_COLUMN_FAMILY, SRC_COLUMN, CommonUtil.stringToBytes(mapper.writeValueAsString(ticket)));

            table.put(put);
        }
    }

    @Override
    public Ticket getById(UUID id) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(CommonUtil.uuidToBytes(id));
            Result result = table.get(get);
            if (result.isEmpty())
                return null;

            byte[] ticketRaw = result.getValue(MAIN_COLUMN_FAMILY, SRC_COLUMN);
            return mapper.readValue(CommonUtil.bytesToString(ticketRaw), Ticket.class);
        }
    }

    public List<Ticket> search(String ownerId, LocalDate from) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            List<Ticket> tickets = new ArrayList<>();

            Scan scan = new Scan();
            scan.addFamily(IDX_COLUMN_FAMILY);
            scan.addFamily(MAIN_COLUMN_FAMILY);

            FilterList filters = new FilterList();
            filters.addFilter(new SingleColumnValueFilter(IDX_COLUMN_FAMILY, OWNER_COLUMN,
                    CompareOperator.EQUAL, CommonUtil.stringToBytes(ownerId)));

            filters.addFilter(new SingleColumnValueFilter(IDX_COLUMN_FAMILY, DT_COLUMN,
                    CompareOperator.GREATER_OR_EQUAL, CommonUtil.offsetDateTimeToBytes(from.atTime(LocalTime.MIDNIGHT).atOffset(ZoneOffset.UTC))));

            scan.setFilter(filters);

            scan.setMaxResultSize(1000);

            for (Result result : table.getScanner(scan)) {
                byte[] ticketRaw = result.getValue(MAIN_COLUMN_FAMILY, SRC_COLUMN);
                tickets.add(mapper.readValue(CommonUtil.bytesToString(ticketRaw), Ticket.class));
            }

            return tickets;
        }
    }
}
