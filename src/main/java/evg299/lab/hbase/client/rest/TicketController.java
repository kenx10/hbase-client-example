package evg299.lab.hbase.client.rest;

import evg299.lab.hbase.client.dao.repo.TicketHbaseRepo;
import evg299.lab.hbase.client.dto.Ticket;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class TicketController {
    private final TicketHbaseRepo ticketHbaseRepo;

    @GetMapping("/ticket/{uuid}")
    public Ticket getById(@PathVariable UUID uuid) {
        try {
            return ticketHbaseRepo.getById(uuid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @PutMapping("/ticket/save")
    public void save(@RequestBody Ticket ticket) {
        try {
            ticketHbaseRepo.save(ticket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
