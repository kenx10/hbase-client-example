package evg299.lab.hbase.client.dao.api;

import evg299.lab.hbase.client.dto.Ticket;

import java.io.IOException;
import java.util.UUID;

public interface TicketDao {
    void deleteById(UUID id) throws IOException;

    void save(Ticket ticket) throws IOException;

    Ticket getById(UUID id) throws IOException;
}
