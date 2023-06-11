package evg299.lab.hbase.client.dao.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import evg299.lab.hbase.client.dao.api.TicketDao;
import evg299.lab.hbase.client.dao.tables.TicketTable;
import evg299.lab.hbase.client.dto.Ticket;
import evg299.lab.hbase.client.util.CommonUtil;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class TicketHbaseRepo extends TicketTable implements TicketDao {
    private final Connection connection;
    private final ObjectMapper mapper;

    public void createTable() throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();

            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(IDX_COLUMN_FAMILY).build())
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(MAIN_COLUMN_FAMILY).build())
                    .build();

            admin.createTable(descriptor);
        } finally {
            if (null != admin)
                admin.close();
        }
    }

    @Override
    public void save(Ticket ticket) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        Put put = new Put(CommonUtil.uuidToBytes(ticket.getUuid()));
        put.addColumn(IDX_COLUMN_FAMILY, DT_COLUMN, CommonUtil.offsetDateTimeToBytes(ticket.getDateTime()));
        put.addColumn(IDX_COLUMN_FAMILY, OWNER_COLUMN, CommonUtil.stringToBytes(ticket.getOwnerId()));
        put.addColumn(IDX_COLUMN_FAMILY, DEVICE_COLUMN, CommonUtil.longToBytes(ticket.getDeviceId()));
        put.addColumn(IDX_COLUMN_FAMILY, SUM_COLUMN, Bytes.toBytes(ticket.getSum()));
        put.addColumn(IDX_COLUMN_FAMILY, REJECTED_COLUMN, CommonUtil.booleanToBytes(ticket.isRejected()));
        put.addColumn(MAIN_COLUMN_FAMILY, SRC_COLUMN, CommonUtil.stringToBytes(mapper.writeValueAsString(ticket)));

        table.put(put);
    }

    @Override
    public Ticket getById(UUID id) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        Get get = new Get(CommonUtil.uuidToBytes(id));
        Result result = table.get(get);
        byte[] ticketRaw = result.getValue(MAIN_COLUMN_FAMILY, SRC_COLUMN);

        return mapper.readValue(CommonUtil.bytesToString(ticketRaw), Ticket.class);
    }


}
