package lat.fab.imires.controller;

import lat.fab.imires.model.UserSummary;
import lat.fab.imires.repository.MongoAcademicRepository;
import lat.fab.imires.repository.MongoClientRepository;
import lat.fab.imires.repository.MongoProviderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequiredArgsConstructor
@Log4j2
public class AdminController {

    private final MongoClientRepository clientRepository;
    private final MongoProviderRepository providerRepository;
    private final MongoAcademicRepository academicRepository;

    // Read-only listing of all system users (clients, providers and academics).
    // Intended for admin visualization only; no fields are mutated here.
    @GetMapping("/admin/users")
    public Flux<UserSummary> getAllUsers() {
        Flux<UserSummary> clients = clientRepository.findAll()
                .map(c -> new UserSummary(c.getId(), c.getEmail(), c.getCompanyName(),
                        "Client", c.getCountries(), c.isAdmin()));

        Flux<UserSummary> providers = providerRepository.findAll()
                .map(p -> new UserSummary(p.getId(), p.getEmail(), p.getCompanyName(),
                        "Provider", p.getCountries(), p.isAdmin()));

        Flux<UserSummary> academics = academicRepository.findAll()
                .map(a -> new UserSummary(a.getId(), a.getEmail(), a.getCompanyName(),
                        "Academic", a.getCountries(), a.isAdmin()));

        return Flux.concat(clients, providers, academics);
    }
}
