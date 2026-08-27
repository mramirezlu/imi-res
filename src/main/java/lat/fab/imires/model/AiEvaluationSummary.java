package lat.fab.imires.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

// Lightweight projection of AiEvaluation for the admin table view.
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AiEvaluationSummary {

    private String id;
    private String companyName;
    private String userEmail;
    private String userType;
    private Integer feedback;
    private Date createdAt;
}
