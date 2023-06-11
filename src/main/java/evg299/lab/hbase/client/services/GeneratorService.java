package evg299.lab.hbase.client.services;

import evg299.lab.hbase.client.dao.repo.TicketHbaseRepo;
import evg299.lab.hbase.client.dto.Ticket;
import evg299.lab.hbase.client.dto.TicketItem;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

@Service
@RequiredArgsConstructor
public class GeneratorService {
    public static final Random RANDOM = new Random();

    private final TicketHbaseRepo ticketHbaseRepo;

    public void createTable() {
        try {
            ticketHbaseRepo.createTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<UUID> generateAndSaveTickets(int count) {
        Map<Integer, UUID> ids = new HashMap<>();
        for (int i = 0; i < count; i++) {
            try {
                Ticket ticket = generateTicket();
                ticketHbaseRepo.save(ticket);
                ids.put(i % 1000, ticket.getUuid());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return ids.values();
    }

    public Ticket generateTicket() {
        Ticket ticket = new Ticket();
        ticket.setUuid(UUID.randomUUID());
        ticket.setDateTime(Instant.ofEpochSecond(RANDOM.nextInt((int) (System.currentTimeMillis() / 1000))).atOffset(ZoneOffset.UTC));
        ticket.setOwnerId("" + (100_000_000 + RANDOM.nextInt(899_999_999)));
        ticket.setDeviceId(RANDOM.nextInt(100_000_000));
        ticket.setNumOnDevice(RANDOM.nextInt(100_000));

        int itemsCount = RANDOM.nextInt(20);
        for (int i = 0; i < itemsCount; i++) {
            ticket.getItems().add(generateTicketItem());
        }

        if (RANDOM.nextBoolean()) {
            ticket.setRejected(true);
            ticket.setRejectUuid(UUID.randomUUID());
            ticket.setRejectDateTime(ticket.getDateTime().plusDays(1));
            ticket.setRejectReason("Reason #" + RANDOM.nextInt(3));
        }
        return ticket;
    }

    public TicketItem generateTicketItem() {
        TicketItem ticketItem = new TicketItem();
        ticketItem.setName("Product-" + RANDOM.nextInt(10_000));
        ticketItem.setCode("C" + RANDOM.nextInt(10_000));
        ticketItem.setPrice(BigDecimal.valueOf(RANDOM.nextInt(100_000)));
        ticketItem.setAmount(BigDecimal.valueOf(RANDOM.nextInt(100)));
        return ticketItem;
    }
}
