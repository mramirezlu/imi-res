package lat.fab.imires.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

// Read-only projection of a system user, used by the admin user listing.
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserSummary {

    private String id;
    private String email;
    private String companyName;
    private String type; // "Client" | "Provider" | "Academic"
    private List<String> countries;
    private boolean admin;
}
