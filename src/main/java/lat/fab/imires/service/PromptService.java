package lat.fab.imires.service;

import lat.fab.imires.model.PromptConfig;
import lat.fab.imires.repository.DataPromptRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Central place for the AI prompts. Only the natural-language "texto editable"
// can be changed by an admin (persisted in PromptConfig). The dynamic variables
// and the required JSON output format stay here in code and are appended by the
// callers, so the parsing logic can never be broken from the UI.
@Service
public class PromptService {

    public static final String KEY_PREGUNTAS = "GENERAR_PREGUNTAS";
    public static final String KEY_ANALISIS = "ANALIZAR_RESPUESTAS";
    public static final String KEY_ASSISTANT = "ASSISTANT_INSTRUCTIONS";

    // Fixed JSON output formats — NOT editable, appended by the callers.
    public static final String FORMATO_PREGUNTAS =
            "{\"preguntas\": {\"Product Management\": [{\"pregunta\": \"...\", \"opciones\": [{\"texto\": \"...\", \"puntaje\": 1}, {\"texto\": \"...\", \"puntaje\": 2}, {\"texto\": \"...\", \"puntaje\": 3}, {\"texto\": \"...\", \"puntaje\": 4}]}], \"Resources Management\": [...], \"Information Management\": [...], \"Organization Management\": [...], \"Innovation Management\": [...]}}";

    public static final String FORMATO_ANALISIS =
            "{\"resumen\": \"...\", \"puntajes\": {\"Product Management\": {\"Product\": {\"puntaje\": N, \"justificacion\": \"...\"}, \"Design\": {...}, \"Fabrication\": {...}}, \"Resources Management\": {\"Materials\": {...}, \"Energy\": {...}, \"Logistics\": {...}}, \"Information Management\": {\"Sensing\": {...}, \"Processing\": {...}, \"Actuator\": {...}}, \"Organization Management\": {\"Organization\": {...}, \"Impact\": {...}, \"Compliance\": {...}}, \"Innovation Management\": {\"Innovation\": {...}, \"Intellectual property\": {...}, \"Training\": {...}}}, \"recomendaciones\": {\"Product Management\": \"...\", \"Resources Management\": \"...\", \"Information Management\": \"...\", \"Organization Management\": \"...\", \"Innovation Management\": \"...\"}, \"recomendacion_general\": \"...\"}";

    private static final Map<String, String> DEFAULTS = new LinkedHashMap<>();
    private static final Map<String, String> NOMBRES = new LinkedHashMap<>();
    private static final Map<String, String> DESCRIPCIONES = new LinkedHashMap<>();
    private static final Map<String, String> VARIABLES_INFO = new LinkedHashMap<>();
    private static final Map<String, String> FORMATO_INFO = new LinkedHashMap<>();

    static {
        // Original prompt texts live in resource files (src/main/resources/prompts/*.txt)
        // so the originals are versioned in the project and never lost when an admin edits them.
        DEFAULTS.put(KEY_PREGUNTAS, loadResource("/prompts/generar-preguntas.txt"));
        NOMBRES.put(KEY_PREGUNTAS, "Preguntas del cuestionario");
        DESCRIPCIONES.put(KEY_PREGUNTAS, "Define cómo la IA crea las preguntas que se le hacen a cada empresa.");
        VARIABLES_INFO.put(KEY_PREGUNTAS, "Se agrega automáticamente al inicio: la descripción de la empresa.");
        FORMATO_INFO.put(KEY_PREGUNTAS, FORMATO_PREGUNTAS);

        DEFAULTS.put(KEY_ANALISIS, loadResource("/prompts/analizar-respuestas.txt"));
        NOMBRES.put(KEY_ANALISIS, "Análisis de resultados");
        DESCRIPCIONES.put(KEY_ANALISIS, "Define cómo la IA evalúa las respuestas y arma los puntajes y las recomendaciones.");
        VARIABLES_INFO.put(KEY_ANALISIS, "Se agregan automáticamente: la descripción de la empresa y las respuestas del usuario.");
        FORMATO_INFO.put(KEY_ANALISIS, FORMATO_ANALISIS);

        DEFAULTS.put(KEY_ASSISTANT, loadResource("/prompts/assistant-instructions.txt"));
        NOMBRES.put(KEY_ASSISTANT, "Comportamiento general de la IA");
        DESCRIPCIONES.put(KEY_ASSISTANT, "Define el tono y las reglas generales que la IA sigue en todas sus respuestas.");
        VARIABLES_INFO.put(KEY_ASSISTANT, "No usa variables; es la instrucción base del asistente.");
        FORMATO_INFO.put(KEY_ASSISTANT, "");
    }

    // Reads an original prompt from the classpath (src/main/resources).
    private static String loadResource(String path) {
        try (var is = PromptService.class.getResourceAsStream(path)) {
            if (is == null) return "";
            return new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            return "";
        }
    }

    private final DataPromptRepository promptRepository;

    public PromptService(DataPromptRepository promptRepository) {
        this.promptRepository = promptRepository;
    }

    public List<String> keys() { return new ArrayList<>(DEFAULTS.keySet()); }
    public boolean isValidKey(String key) { return DEFAULTS.containsKey(key); }
    public String nombre(String key) { return NOMBRES.get(key); }
    public String descripcion(String key) { return DESCRIPCIONES.get(key); }
    public String variablesInfo(String key) { return VARIABLES_INFO.get(key); }
    public String formatoInfo(String key) { return FORMATO_INFO.get(key); }
    public String defaultText(String key) { return DEFAULTS.get(key); }

    // Effective editable text: the stored override if present/non-blank, else the default.
    public Mono<String> getText(String key) {
        String def = DEFAULTS.getOrDefault(key, "");
        return promptRepository.findById(key)
                .map(PromptConfig::getTextoEditable)
                .filter(t -> t != null && !t.isBlank())
                .defaultIfEmpty(def);
    }
}
