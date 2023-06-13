package evg299.lab.hbase.client.rest;

import evg299.lab.hbase.client.dao.repo.TicketHbaseRepo;
import evg299.lab.hbase.client.dto.Ticket;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class TicketController {
    private final TicketHbaseRepo ticketHbaseRepo;

    @GetMapping("/ticket/search")
    public List<Ticket> search(@RequestParam String ownerId,
                               @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) @RequestParam LocalDate from) {
        try {
            return ticketHbaseRepo.search(ownerId, from);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/ticket/{uuid}")
    public Ticket getById(@PathVariable UUID uuid) {
        try {
            return ticketHbaseRepo.getById(uuid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @PutMapping("/ticket/")
    public void save(@RequestBody Ticket ticket) {
        try {
            ticketHbaseRepo.save(ticket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @DeleteMapping("/ticket/{uuid}")
    public void deleteById(@PathVariable UUID uuid) {
        try {
            ticketHbaseRepo.deleteById(uuid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
