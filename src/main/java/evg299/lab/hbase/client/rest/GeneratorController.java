package evg299.lab.hbase.client.rest;

import evg299.lab.hbase.client.services.GeneratorService;
import io.swagger.v3.oas.annotations.Parameter;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class GeneratorController {
    private final GeneratorService generatorService;

    @PostMapping("/generate/table/tickets")
    public void generateTableTickets() {
        generatorService.createTable();
    }

    @PostMapping("/generate/tickets")
    public Collection<UUID> generateTickets(@Parameter(example = "1000") @RequestParam int count) {
        return generatorService.generateAndSaveTickets(count);
    }
}
