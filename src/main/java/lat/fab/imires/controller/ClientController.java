package lat.fab.imires.controller;

import lat.fab.imires.model.Client;
import lat.fab.imires.model.Imi;
import lat.fab.imires.model.Provider;
import lat.fab.imires.repository.MongoClientRepository;
import lat.fab.imires.repository.MongoProviderRepository;
import lat.fab.imires.util.Util;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@Log4j2
public class ClientController {

    private final MongoClientRepository clientRepository;
    private final MongoProviderRepository providerRepository;
    private final AtomicLong counter = new AtomicLong();

    // Affinity tier width: providers whose affinity falls in the same tier are
    // considered "similar" and then ordered by proximity. Adjustable.
    private static final int AFFINITY_BUCKET = 3;

    public ClientController(
            MongoClientRepository clientRepository,
            MongoProviderRepository providerRepository) {
        this.clientRepository = clientRepository;
        this.providerRepository = providerRepository;
    }

    @GetMapping("/")
    public String healthCheck() {
        return String.format("Hello World! #%s", counter.incrementAndGet());
    }

    @PostMapping("/clients")
    public Mono<Client> create(@RequestBody Client c) {
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
        c.setImis(List.of(defaultImi));
        return clientRepository.save(c);
    }

    @GetMapping("/clients/{email}")
    public Mono<Client> findByEmail(@PathVariable String email) {
        return clientRepository.findByEmail(email);
    }

    @PutMapping("/clients/{idClient}")
    public Mono<Client> update(@PathVariable String idClient, @RequestBody Client client) {
        return clientRepository.findById(idClient)
                .flatMap(c -> {
                    client.setImis(c.getImis());
                    client.setAdmin(c.isAdmin()); // preserve admin role on profile update
                    return clientRepository.save(client);
                });
    }

    @PutMapping("/clients/{idClient}/update-imi")
    public Mono<Client> updateImi(@PathVariable String idClient, @RequestBody Map<Integer, Integer> vars) {
        // get first day of current month date
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        // manipulate date
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date today = cal.getTime();

        return clientRepository.findById(idClient)
                .flatMap(c -> {
                    // never executes because imis is never empty
                    /*if (c.getImis().isEmpty()) { // if empty imi list, last imi does not exist
                        Imi newImi = new Imi(vars, today);
                        c.getImis().add(newImi);
                        return clientRepository.save(c);
                    } */

                    // create or update imi with today's month
                    Calendar calendar1 = Calendar.getInstance();
                    calendar1.setTime(today);

                    Date lastDate = c.getImis().get(c.getImis().size() - 1).getDateTime();
                    Calendar calendar2 = Calendar.getInstance();
                    calendar2.setTime(lastDate);

                    // if imi of current month exists, override it
                    if (calendar1.get(Calendar.MONTH) == calendar2.get(Calendar.MONTH) &&
                            calendar1.get(Calendar.YEAR) == calendar2.get(Calendar.YEAR)) {
                        c.getImis().get(c.getImis().size() - 1).setVars(vars);
                    } else {
                        Imi newImi = new Imi(vars, today);
                        c.getImis().add(newImi);
                    }
                    return clientRepository.save(c);
                });
    }

    @GetMapping("/clients/{idClient}/get-suggestions")
    public Flux<Provider> getSuggestions(@PathVariable String idClient) {
        var wrapper = new Object(){ Client client; };

        return this.clientRepository.findById(idClient)
                .flatMapMany(c -> {
                    wrapper.client = c;
                    return this.providerRepository.findSuggestedProviders(c, 3);
                })
                .sort((p1, p2) -> {
                    // Rank by affinity first (who fills the most gap); once we have
                    // coordinates, order providers of similar affinity by proximity.
                    Client client = wrapper.client;
                    Imi clientLastImi = client.getImis().get(client.getImis().size() - 1);

                    int aff1 = affinity(p1, clientLastImi);
                    int aff2 = affinity(p2, clientLastImi);

                    boolean clientHasCoords = client.getLatitude() != null && client.getLongitude() != null;
                    if (!clientHasCoords) {
                        return aff2 - aff1; // no client location → affinity only (previous behavior)
                    }

                    // Affinity by tiers; within the same tier, the closest provider first.
                    int level1 = aff1 / AFFINITY_BUCKET;
                    int level2 = aff2 / AFFINITY_BUCKET;
                    if (level1 != level2) {
                        return level2 - level1; // higher affinity tier first
                    }
                    double d1 = distance(client, p1);
                    double d2 = distance(client, p2);
                    if (Double.compare(d1, d2) != 0) {
                        return Double.compare(d1, d2); // closer first (providers w/o coords sink to the end)
                    }
                    return aff2 - aff1; // finer affinity tie-break within same tier & distance
                })
                .take(8) // best 8 providers
                .map(p -> {
                    Client client = wrapper.client;
                    if (client.getLatitude() != null && client.getLongitude() != null
                            && p.getLatitude() != null && p.getLongitude() != null) {
                        double km = haversineKm(client.getLatitude(), client.getLongitude(),
                                p.getLatitude(), p.getLongitude());
                        p.setDistanceKm((int) Math.round(km));
                    }
                    return p;
                });
    }

    // Affinity = how much the provider exceeds the client across all IMI areas (sum of positive diffs).
    private int affinity(Provider p, Imi clientLastImi) {
        int sum = 0;
        Imi last = p.getImis().get(p.getImis().size() - 1);
        for (Map.Entry<Integer, Integer> entry : last.getVars().entrySet()) {
            int diff = entry.getValue() - clientLastImi.getVars().get(entry.getKey());
            if (diff > 0) {
                sum += diff;
            }
        }
        return sum;
    }

    // Distance in km between client and provider; MAX_VALUE if the provider has no coordinates.
    private double distance(Client c, Provider p) {
        if (p.getLatitude() == null || p.getLongitude() == null) {
            return Double.MAX_VALUE;
        }
        return haversineKm(c.getLatitude(), c.getLongitude(), p.getLatitude(), p.getLongitude());
    }

    private double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double earthRadiusKm = 6371;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // returns array containing 5 axes for radar chart
    @GetMapping("/clients/get-country-imi/{country}")
    public Mono<DimensionForChart[]> getCountryImi(@PathVariable String country) {
        List<Client> clientList = this.clientRepository.findByCountriesContaining(country)
                .collectList().share().block();
        List<Provider> providerList = this.providerRepository.findByCountriesContaining(country)
                .collectList().share().block();

        int sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0;
        int n = clientList.size() + providerList.size();

        if (n == 0) { return Mono.empty(); }

        for (Client c : clientList) {
            Imi lastImi = c.getImis().get(c.getImis().size() - 1);
            sum1 += lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3);
            sum2 += lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6);
            sum3 += lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9);
            sum4 += lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12);
            sum5 += lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15);
        }

        for (Provider p : providerList) {
            Imi lastImi = p.getImis().get(p.getImis().size() - 1);
            sum1 += lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3);
            sum2 += lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6);
            sum3 += lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9);
            sum4 += lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12);
            sum5 += lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15);
        }

        DimensionForChart[] result = new DimensionForChart[5];
        result[0] = new DimensionForChart();
        result[0].axis = Util.DIMENSION_TO_NAME_MAP.get(1);
        result[0].value = (float) sum1 / (n * 3);
        result[1] = new DimensionForChart();
        result[1].axis = Util.DIMENSION_TO_NAME_MAP.get(2);
        result[1].value = (float) sum2 / (n * 3);
        result[2] = new DimensionForChart();
        result[2].axis = Util.DIMENSION_TO_NAME_MAP.get(3);
        result[2].value = (float) sum3 / (n * 3);
        result[3] = new DimensionForChart();
        result[3].axis = Util.DIMENSION_TO_NAME_MAP.get(4);
        result[3].value = (float) sum4 / (n * 3);
        result[4] = new DimensionForChart();
        result[4].axis = Util.DIMENSION_TO_NAME_MAP.get(5);
        result[4].value = (float) sum5 / (n * 3);

        return Mono.just(result);
    }

    @GetMapping("/clients/get-country-imi")
    public Mono<Map> getAllCountryImi() {
        List<Client> clientList = this.clientRepository.findAll()
                .collectList().share().block();
        List<Provider> providerList = this.providerRepository.findAll()
                .collectList().share().block();

        Map<String, Object[]> countriesWithImi = new HashMap<>();
        // counter by country
        Map<String, Integer> counter = new HashMap<>();

        // populate with country codes
        /*Util.COUNTRY_TO_CODE_MAP.entrySet().forEach(entry -> {
            DimensionForChart[] dimensions = new DimensionForChart[6];
            dimensions[0] = new DimensionForChart("all", 0);
            dimensions[1] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(1), 0);
            dimensions[2] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(2), 0);
            dimensions[3] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(3), 0);
            dimensions[4] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(4), 0);
            dimensions[5] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(5), 0);

            countriesWithImi.put(entry.getValue(), dimensions);
            counter.put(entry.getValue(), 0);
        });*/

        for (Client client : clientList) {
            Imi lastImi = client.getImis().get(client.getImis().size() - 1);
            float avg1 = (float) (lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3)) / 3;
            float avg2 = (float) (lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6)) / 3;
            float avg3 = (float) (lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9)) / 3;
            float avg4 = (float) (lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12)) / 3;
            float avg5 = (float) (lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15)) / 3;
            float avg0 = (avg1 + avg2 + avg3 + avg4 + avg5) / 5;

            for (String country : client.getCountries()) { // count this user's imi for each of user's country
                if (!Util.COUNTRY_TO_CODE_MAP.containsKey(country)) {
                    System.out.println("Country key not found: " + country);
                    continue;
                } else if (!countriesWithImi.containsKey(Util.COUNTRY_TO_CODE_MAP.get(country))) {
                    Object[] dimensions = new Object[7];
                    dimensions[0] = new DimensionForChart("all", 0);
                    dimensions[1] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(1), 0);
                    dimensions[2] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(2), 0);
                    dimensions[3] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(3), 0);
                    dimensions[4] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(4), 0);
                    dimensions[5] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(5), 0);

                    countriesWithImi.put(Util.COUNTRY_TO_CODE_MAP.get(country), dimensions);
                    counter.put(Util.COUNTRY_TO_CODE_MAP.get(country), 0);
                }

                String countryCode = Util.COUNTRY_TO_CODE_MAP.get(country);
                Object[] dims = countriesWithImi.get(countryCode);
                // add values to current ones
                ((DimensionForChart) dims[0]).value += avg0;
                ((DimensionForChart) dims[1]).value += avg1;
                ((DimensionForChart) dims[2]).value += avg2;
                ((DimensionForChart) dims[3]).value += avg3;
                ((DimensionForChart) dims[4]).value += avg4;
                ((DimensionForChart) dims[5]).value += avg5;

                counter.put(countryCode, counter.get(countryCode) + 1);
            }
        }

        for (Provider provider : providerList) {
            Imi lastImi = provider.getImis().get(provider.getImis().size() - 1);
            float avg1 = (float) (lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3)) / 3;
            float avg2 = (float) (lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6)) / 3;
            float avg3 = (float) (lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9)) / 3;
            float avg4 = (float) (lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12)) / 3;
            float avg5 = (float) (lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15)) / 3;
            float avg0 = (avg1 + avg2 + avg3 + avg4 + avg5) / 5;

            for (String country : provider.getCountries()) { // count this user's imi for each of user's country
                if (!Util.COUNTRY_TO_CODE_MAP.containsKey(country)) {
                    System.out.println("Country key not found: " + country);
                    continue;
                } else if (!countriesWithImi.containsKey(Util.COUNTRY_TO_CODE_MAP.get(country)))  {
                    Object[] dimensions = new Object[7];
                    dimensions[0] = new DimensionForChart("all", 0);
                    dimensions[1] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(1), 0);
                    dimensions[2] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(2), 0);
                    dimensions[3] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(3), 0);
                    dimensions[4] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(4), 0);
                    dimensions[5] = new DimensionForChart(Util.DIMENSION_TO_NAME_MAP.get(5), 0);

                    countriesWithImi.put(Util.COUNTRY_TO_CODE_MAP.get(country), dimensions);
                    counter.put(Util.COUNTRY_TO_CODE_MAP.get(country), 0);
                }

                String countryCode = Util.COUNTRY_TO_CODE_MAP.get(country);
                Object[] dims = countriesWithImi.get(countryCode);
                // add values to current ones
                ((DimensionForChart) dims[0]).value += avg0;
                ((DimensionForChart) dims[1]).value += avg1;
                ((DimensionForChart) dims[2]).value += avg2;
                ((DimensionForChart) dims[3]).value += avg3;
                ((DimensionForChart) dims[4]).value += avg4;
                ((DimensionForChart) dims[5]).value += avg5;

                counter.put(countryCode, counter.get(countryCode) + 1);
            }
        }

        // Computing averages
        for (var entry : countriesWithImi.entrySet()) {
            int n = counter.get(entry.getKey());
            if (n < 1) continue;

            Object[] dims = entry.getValue();
            ((DimensionForChart) dims[0]).value /= n;
            ((DimensionForChart) dims[1]).value /= n;
            ((DimensionForChart) dims[2]).value /= n;
            ((DimensionForChart) dims[3]).value /= n;
            ((DimensionForChart) dims[4]).value /= n;
            ((DimensionForChart) dims[5]).value /= n;

            // include count per country
            entry.getValue()[6] = Map.of("count", n);
        }

        return Mono.just(countriesWithImi);
    }

    // returns array containing 5 axes for radar chart
    @GetMapping("/clients/get-industry-imi/{industry}")
    public Mono<DimensionForChart[]> getIndustryImi(@PathVariable String industry) {
        List<Client> clientList = this.clientRepository.findByIndustriesContaining(industry)
                .collectList().share().block();

        List<Provider> providerList = this.providerRepository.findByIndustriesContaining(industry)
                .collectList().share().block();

        int sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0;
        int n = clientList.size() + providerList.size();

        if (n == 0) { return Mono.empty(); }

        for (Client c : clientList) {
            Imi lastImi = c.getImis().get(c.getImis().size() - 1);
            sum1 += lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3);
            sum2 += lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6);
            sum3 += lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9);
            sum4 += lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12);
            sum5 += lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15);
        }

        for (Provider p : providerList) {
            Imi lastImi = p.getImis().get(p.getImis().size() - 1);
            sum1 += lastImi.getVars().get(1) + lastImi.getVars().get(2) + lastImi.getVars().get(3);
            sum2 += lastImi.getVars().get(4) + lastImi.getVars().get(5) + lastImi.getVars().get(6);
            sum3 += lastImi.getVars().get(7) + lastImi.getVars().get(8) + lastImi.getVars().get(9);
            sum4 += lastImi.getVars().get(10) + lastImi.getVars().get(11) + lastImi.getVars().get(12);
            sum5 += lastImi.getVars().get(13) + lastImi.getVars().get(14) + lastImi.getVars().get(15);
        }

        DimensionForChart[] result = new DimensionForChart[5];
        result[0] = new DimensionForChart();
        result[0].axis = Util.DIMENSION_TO_NAME_MAP.get(1);
        result[0].value = (float) sum1 / (n * 3);
        result[1] = new DimensionForChart();
        result[1].axis = Util.DIMENSION_TO_NAME_MAP.get(2);
        result[1].value = (float) sum2 / (n * 3);
        result[2] = new DimensionForChart();
        result[2].axis = Util.DIMENSION_TO_NAME_MAP.get(3);
        result[2].value = (float) sum3 / (n * 3);
        result[3] = new DimensionForChart();
        result[3].axis = Util.DIMENSION_TO_NAME_MAP.get(4);
        result[3].value = (float) sum4 / (n * 3);
        result[4] = new DimensionForChart();
        result[4].axis = Util.DIMENSION_TO_NAME_MAP.get(5);
        result[4].value = (float) sum5 / (n * 3);

        return Mono.just(result);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class DimensionForChart {
        String axis;
        float value;
    }
}
