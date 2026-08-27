package lat.fab.imires.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

// One stored iteration of the AI-assisted IMI evaluation:
// who ran it, the company context, the generated questions,
// the user's choices and the AI analysis result.
@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AiEvaluation {

    @Id
    private String id;
    private String userId;
    private String userEmail;
    private String companyName;
    private String userType; // "Client" | "Provider" | "Academic"
    private String descripcion;
    private Object preguntas;   // AI-generated questionnaire (raw)
    private Object respuestas;  // user's chosen answers (raw)
    private Object resultado;   // AI analysis output (resumen, puntajes, recomendaciones...)
    private Integer feedback;   // user satisfaction rating 1-5 (null until given)
    private Date createdAt;
}
