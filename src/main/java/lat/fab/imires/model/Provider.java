package lat.fab.imires.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Provider {

    @Id
    private String id;
    private String email;
    private String companyName;
    private String description;
    private String phone;
    private String website;
    private List<String> extraUrls;
    private List<String> countries;
    private List<String> industries;
    private List<Imi> imis;
    private Map<Integer, List<String>> services;
    private boolean admin;
    private Double latitude;
    private Double longitude;
    @Transient
    private Integer distanceKm; // computed for suggestions only, never persisted

}
