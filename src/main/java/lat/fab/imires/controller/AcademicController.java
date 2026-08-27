package lat.fab.imires.controller;

import lat.fab.imires.model.Academic;
import lat.fab.imires.model.Imi;
import lat.fab.imires.repository.MongoAcademicRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.*;

@RestController
@RequiredArgsConstructor
@Log4j2
public class AcademicController {

    private final MongoAcademicRepository academicRepository;

    @PostMapping("/academics")
    public Mono<Academic> create(@RequestBody Academic a) {
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
        a.setImis(List.of(defaultImi));

        return academicRepository.save(a);
    }

    @GetMapping("/academics/{email}")
    public Mono<Academic> findByEmail(@PathVariable String email) {
        return academicRepository.findByEmail(email);
    }

    @PutMapping("/academics/{idAcademic}")
    public Mono<Academic> update(@PathVariable String idAcademic, @RequestBody Academic academic) {
        return academicRepository.findById(idAcademic)
                .flatMap(a -> {
                    academic.setImis(a.getImis());
                    academic.setAdmin(a.isAdmin()); // preserve admin role on profile update
                    return academicRepository.save(academic);
                });
    }

    @PutMapping("/academics/{idAcademic}/update-imi")
    public Mono<Academic> updateImi(@PathVariable String idAcademic, @RequestBody Map<Integer, Integer> vars) {
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

        return academicRepository.findById(idAcademic)
                .flatMap(a -> {
                    /*if (a.getImis().isEmpty()) { // if empty imi list, last imi does not exist
                        Imi newImi = new Imi(vars, today);
                        a.getImis().add(newImi);
                        return providerRepository.save(a);
                    } */

                    // create or update imi with today's month
                    Calendar calendar1 = Calendar.getInstance();
                    calendar1.setTime(today);

                    Date lastDate = a.getImis().get(a.getImis().size() - 1).getDateTime();
                    Calendar calendar2 = Calendar.getInstance();
                    calendar2.setTime(lastDate);

                    // if imi of current month exists, override it
                    if (calendar1.get(Calendar.MONTH) == calendar2.get(Calendar.MONTH) &&
                            calendar1.get(Calendar.YEAR) == calendar2.get(Calendar.YEAR)) {
                        a.getImis().get(a.getImis().size() - 1).setVars(vars);
                    } else {
                        Imi newImi = new Imi(vars, today);
                        a.getImis().add(newImi);
                    }
                    return academicRepository.save(a);
                });
    }
}
