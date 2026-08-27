package lat.fab.imires.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

// Stores the admin-editable TEXT of an AI prompt. The dynamic variables and the
// required JSON format stay in code (see PromptService) and are never stored here.
@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PromptConfig {

    @Id
    private String key; // GENERAR_PREGUNTAS | ANALIZAR_RESPUESTAS | ASSISTANT_INSTRUCTIONS
    private String textoEditable;
    private Date updatedAt;
}
