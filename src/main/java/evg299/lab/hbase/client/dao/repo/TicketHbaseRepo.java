package evg299.lab.hbase.client.dao.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import evg299.lab.hbase.client.dao.api.TicketDao;
import evg299.lab.hbase.client.dao.tables.TicketTable;
import evg299.lab.hbase.client.dto.Ticket;
import evg299.lab.hbase.client.util.CommonUtil;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                descriptor.addFamily(new HColumnDescriptor(IDX_COLUMN_FAMILY));
                descriptor.addFamily(new HColumnDescriptor(MAIN_COLUMN_FAMILY));
                admin.createTable(descriptor);
            }
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

    @Override
    public Ticket reject(UUID id, UUID rejectId, OffsetDateTime rejectDateTime, String rejectReason) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(CommonUtil.uuidToBytes(id));
            Result result = table.get(get);
            if (result.isEmpty())
                return null;

            byte[] ticketRaw = result.getValue(MAIN_COLUMN_FAMILY, SRC_COLUMN);
            Ticket ticket = mapper.readValue(CommonUtil.bytesToString(ticketRaw), Ticket.class);
            ticket.setRejectUuid(rejectId);
            ticket.setRejectDateTime(rejectDateTime);
            ticket.setRejectReason(rejectReason);

            Put put = new Put(CommonUtil.uuidToBytes(ticket.getUuid()));
            put.addColumn(IDX_COLUMN_FAMILY, REJECTED_COLUMN, CommonUtil.booleanToBytes(ticket.isRejected()));
            put.addColumn(MAIN_COLUMN_FAMILY, SRC_COLUMN, CommonUtil.stringToBytes(mapper.writeValueAsString(ticket)));

            table.put(put);

            return ticket;
        }
    }

    public List<Ticket> search(String ownerId, LocalDate from) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            List<Ticket> tickets = new ArrayList<>();

            Scan scan = new Scan();
            scan.addFamily(IDX_COLUMN_FAMILY);
            scan.addFamily(MAIN_COLUMN_FAMILY);

            FilterList filters = new FilterList();
            if (!StringUtils.isEmpty(ownerId)) {
                filters.addFilter(new SingleColumnValueFilter(IDX_COLUMN_FAMILY, OWNER_COLUMN,
                        CompareFilter.CompareOp.EQUAL, CommonUtil.stringToBytes(ownerId)));
            }

            if (null != from) {
                filters.addFilter(new SingleColumnValueFilter(IDX_COLUMN_FAMILY, DT_COLUMN,
                        CompareFilter.CompareOp.GREATER_OR_EQUAL, CommonUtil.offsetDateTimeToBytes(from.atTime(LocalTime.MIDNIGHT).atOffset(ZoneOffset.UTC))));
            }

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
