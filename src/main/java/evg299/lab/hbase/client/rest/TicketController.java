package evg299.lab.hbase.client.rest;

import evg299.lab.hbase.client.dao.repo.TicketHbaseRepo;
import evg299.lab.hbase.client.dto.Ticket;
import io.swagger.v3.oas.annotations.Parameter;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class TicketController {
    private final TicketHbaseRepo ticketHbaseRepo;

    @GetMapping("/ticket/search")
    public List<Ticket> search(@RequestParam(required = false) String ownerId,
                               @Parameter(example = "2023-05-05") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) @RequestParam(required = false) LocalDate from) {
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

    @PostMapping("/ticket/reject")
    public Ticket reject(@RequestParam UUID id,
                         @RequestParam UUID rejectId,
                         @Parameter(example = "2000-10-31T01:30:00.000-05:00") @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime rejectDateTime,
                         @RequestParam String rejectReason) {
        try {
            return ticketHbaseRepo.reject(id, rejectId, rejectDateTime, rejectReason);
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
