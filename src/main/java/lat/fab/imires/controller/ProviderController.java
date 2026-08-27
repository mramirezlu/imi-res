package lat.fab.imires.controller;

import lat.fab.imires.model.Imi;
import lat.fab.imires.model.Provider;
import lat.fab.imires.repository.MongoProviderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.*;

@RestController
@RequiredArgsConstructor
@Log4j2
public class ProviderController {

    private final MongoProviderRepository providerRepository;

    @PostMapping("/providers")
    public Mono<Provider> create(@RequestBody Provider p) {
        // default imi
        Imi defaultImi = new Imi(
                Map.ofEntries(
                        new AbstractMap.SimpleEntry<Integer, Integer>(1, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(2, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(3, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(4, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(5, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(6, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(7, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(8, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(9, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(10, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(11, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(12, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(13, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(14, 2),
                        new AbstractMap.SimpleEntry<Integer, Integer>(15, 2)
                ),
                new Date()
        );
        p.setImis(List.of(defaultImi));

        // initialize p.services map
        p.setServices(
                Map.ofEntries(
                        new AbstractMap.SimpleEntry<Integer, List<String>>(1, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(2, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(3, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(4, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(5, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(6, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(7, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(8, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(9, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(10, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(11, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(12, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(13, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(14, List.of()),
                        new AbstractMap.SimpleEntry<Integer, List<String>>(15, List.of())
                )
        );
        return providerRepository.save(p);
    }

    @GetMapping("/providers/{email}")
    public Mono<Provider> findByEmail(@PathVariable String email) {
        return providerRepository.findByEmail(email);
    }

    @PutMapping("/providers/{idProvider}")
    public Mono<Provider> update(@PathVariable String idProvider, @RequestBody Provider provider) {
        return providerRepository.findById(idProvider)
                .flatMap(p -> {
                    provider.setImis(p.getImis());
                    provider.setServices(p.getServices());
                    provider.setAdmin(p.isAdmin()); // preserve admin role on profile update
                    return providerRepository.save(provider);
                });
    }

    @PutMapping("/providers/{idProvider}/update-services")
    public Mono<Provider> updateServices(@PathVariable String idProvider, @RequestBody Map<Integer, List<String>> services) {
        return providerRepository.findById(idProvider)
                .flatMap(p -> {
                    log.info(services);
                    p.setServices(services);
                    return providerRepository.save(p);
                });
    }

    @PutMapping("/providers/{idProvider}/update-imi")
    public Mono<Provider> updateImi(@PathVariable String idProvider, @RequestBody Map<Integer, Integer> vars) {
        // get first day of current month date
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        // manipulate date
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date today = c.getTime();

        return providerRepository.findById(idProvider)
                .flatMap(p -> {
                    /*if (p.getImis().isEmpty()) { // if empty imi list, last imi does not exist
                        Imi newImi = new Imi(vars, today);
                        p.getImis().add(newImi);
                        return providerRepository.save(p);
                    } */

                    // create or update imi with today's month
                    Calendar calendar1 = Calendar.getInstance();
                    calendar1.setTime(today);

                    Date lastDate = p.getImis().get(p.getImis().size() - 1).getDateTime();
                    Calendar calendar2 = Calendar.getInstance();
                    calendar2.setTime(lastDate);

                    // if imi of current month exists, override it
                    if (calendar1.get(Calendar.MONTH) == calendar2.get(Calendar.MONTH) &&
                        calendar1.get(Calendar.YEAR) == calendar2.get(Calendar.YEAR)) {
                        p.getImis().get(p.getImis().size() - 1).setVars(vars);
                    } else {
                        Imi newImi = new Imi(vars, today);
                        p.getImis().add(newImi);
                    }
                    return providerRepository.save(p);
                });
    }

    @GetMapping("/providers/{idProvider}/update-imi-test")
    public Mono<Provider> updateImiTest(@PathVariable String idProvider) {
        Map<Integer, Integer> vars = Map.ofEntries(
                new AbstractMap.SimpleEntry<Integer, Integer>(1, 1),
                new AbstractMap.SimpleEntry<Integer, Integer>(2, 2),
                new AbstractMap.SimpleEntry<Integer, Integer>(3, 1),
                new AbstractMap.SimpleEntry<Integer, Integer>(4, 3),
                new AbstractMap.SimpleEntry<Integer, Integer>(5, 3),
                new AbstractMap.SimpleEntry<Integer, Integer>(6, 5),
                new AbstractMap.SimpleEntry<Integer, Integer>(7, 4),
                new AbstractMap.SimpleEntry<Integer, Integer>(8, 4),
                new AbstractMap.SimpleEntry<Integer, Integer>(9, 4),
                new AbstractMap.SimpleEntry<Integer, Integer>(10, 2),
                new AbstractMap.SimpleEntry<Integer, Integer>(11, 1),
                new AbstractMap.SimpleEntry<Integer, Integer>(12, 1),
                new AbstractMap.SimpleEntry<Integer, Integer>(13, 1),
                new AbstractMap.SimpleEntry<Integer, Integer>(14, 0),
                new AbstractMap.SimpleEntry<Integer, Integer>(15, 1)
        );

        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        // manipulate date
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.MONTH, c.get(Calendar.MONTH) + 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Date toRegister = c.getTime();

        return providerRepository.findById(idProvider)
                .flatMap(p -> {
                    Imi newImi = new Imi(vars, toRegister);
                    p.getImis().add(newImi);
                    return providerRepository.save(p);
                });
    }
}
