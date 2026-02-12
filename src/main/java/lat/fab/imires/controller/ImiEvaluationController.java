package lat.fab.imires.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lat.fab.imires.util.OpenAiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/api/imi")
@Slf4j
public class ImiEvaluationController {

    private final OpenAiClient openAiClient;
    private final String openAiModel;
    private final String vectorStoreId;
    private final String assistantId;

    public ImiEvaluationController(OpenAiClient openAiClient,
                                   @org.springframework.beans.factory.annotation.Value("${openai.model}") String openAiModel,
                                   @org.springframework.beans.factory.annotation.Value("${openai.vector-store-id}") String vectorStoreId,
                                   @org.springframework.beans.factory.annotation.Value("${openai.assistant-id:}") String assistantId) {
        this.openAiClient = openAiClient;
        this.openAiModel = openAiModel;
        this.vectorStoreId = vectorStoreId;
        this.assistantId = assistantId;
    }
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<String> fileIds = List.of(
            "file-GWY6eyeK138BT2Mz6uC9aQ",
            "file-2z2bvEjKjspeeCvJpEcCpF",
            "file-7E9bArWsJ54JJw9kzXaVdP"
    );

    // =================== 1. Cargar archivos ===================
    @PostMapping("/cargar-archivos")
    public Mono<ResponseEntity<?>> cargarArchivosDesdeStorage() {
        List<String> archivos = List.of(
                "1_IMI_Detalle_del_Modelo_de_Madurez.pdf",
                "3_IMI_Brechas_identificadas_en_el_Plan_de_Establecimiento_y_Desarrollo_del_CSTD.pdf",
                "4_IMI_Descripción_de_la_cadena_productiva.pdf"
        );

        return Flux.fromIterable(archivos)
                .flatMap(nombre -> {
                    Path path = Paths.get("storage/app/public", nombre);
                    if (!Files.exists(path)) {
                        log.error("Archivo no encontrado: {}", path);
                        return Mono.empty();
                    }
                    try {
                        byte[] contenido = Files.readAllBytes(path);
                        return openAiClient.multipart("/files", contenido, nombre, Map.of("purpose", "assistants"))
                                .map(resp -> resp.path("id").asText());
                    } catch (IOException e) {
                        return Mono.error(e);
                    }
                })
                .collectList()
                .map(ids -> {
                    log.info("Archivos cargados en OpenAI: {}", ids);
                    return ResponseEntity.ok(Map.of("file_ids", ids));
                });
    }

    // =================== 2. Crear VectorStore ===================
    @PostMapping("/crear-vector-store")
    public Mono<ResponseEntity<?>> crearVectorStoreConArchivos() {
        return openAiClient.post("/vector_stores", Map.of("file_ids", fileIds))
                .map(resp -> resp.path("id").asText())
                .map(id -> {
                    log.info("Vector store creado: {}", id);
                    return ResponseEntity.ok(Map.of("vector_store_id", id));
                });
    }

    // =================== 2.5 Crear Assistant permanente ===================
    @PostMapping("/crear-assistant")
    public Mono<ResponseEntity<?>> crearAssistantPermanente() {
        String instructions = """
            Eres un experto en madurez industrial basado en el modelo IMI (Industrial Maturity Index).

            REGLAS:
            - Responde SIEMPRE en español claro y sencillo.
            - No uses tecnicismos complejos ni palabras como "sistémico", "framework" o "automatización avanzada".
            - Devuelve SOLO JSON válido, sin explicaciones ni marcas de Markdown como ```json.
            - Usa los documentos del IMI (modelo de madurez, brechas, cadenas productivas) para fundamentar tus respuestas.
            """;

        return openAiClient.post("/assistants", Map.of(
                "model", openAiModel,
                "name", "IMI Evaluador",
                "instructions", instructions,
                "tools", List.of(Map.of("type", "file_search"))
        )).map(resp -> {
            String id = resp.path("id").asText();
            log.info("Assistant permanente creado: {} - Guarda este ID en openai.assistant-id", id);
            return ResponseEntity.ok(Map.of(
                    "assistant_id", id,
                    "mensaje", "Guarda este ID en application.properties: openai.assistant-id=" + id
            ));
        });
    }

    // =================== 3. Analizar Empresa ===================
    @PostMapping("/analizarEmpresa")
    public Mono<ResponseEntity<?>> analizarEmpresa(@RequestBody Map<String, String> body) {
        String descripcion = body.get("descripcion");

        String prompt = """
                Empresa: """ + descripcion + """

                Analiza con documentos IMI. JSON requerido:
                {"resumen": "...", "puntajes": {"producto": {"puntaje": N, "justificacion": "..."}, "recursos": {...}, "informacion": {...}, "organizacion": {...}, "innovacion": {...}}, "preguntas": {"recursos": [{"pregunta": "...", "opciones": [{"texto": "...", "puntaje": 1}, {"texto": "...", "puntaje": 2}, {"texto": "...", "puntaje": 3}, {"texto": "...", "puntaje": 4}]}], "organizacion": [...], "innovacion": [...]}}
                """;

        return obtenerAssistantId()
                .flatMap(assId -> crearThread()
                        .flatMap(threadId -> enviarMensaje(threadId, prompt)
                                .then(iniciarRun(assId, threadId))
                                .flatMap(runId -> esperarRun(threadId, runId)
                                        .then(obtenerRespuesta(threadId))
                                        .map(output -> ResponseEntity.ok(
                                                Map.of("output", limpiarJson(output))
                                        ))
                                )
                        )
                );
    }

    // =================== 4. Generar Preguntas ===================
    @PostMapping("/generarPreguntas")
    public Mono<ResponseEntity<?>> generarPreguntas(@RequestBody Map<String, String> body) {
        String descripcion = body.get("descripcion");

        String prompt = """
                Descripción de la empresa:
                """ + descripcion + """

                Categorías IMI a evaluar:
                1. Product Management (Product, Design, Fabrication)
                2. Resources Management (Materials, Energy, Logistics)
                3. Information Management (Sensing, Processing, Actuator)
                4. Organization Management (Organization, Impact, Compliance)
                5. Innovation Management (Innovation, Intellectual property, Training)

                IMPORTANTE: Genera MÍNIMO 1 pregunta por CADA categoría (las 5 categorías deben tener al menos 1 pregunta). Cada pregunta con 4 opciones (puntaje 1-4).

                Formato JSON requerido:
                {"preguntas": {"Product Management": [{"pregunta": "...", "opciones": [{"texto": "...", "puntaje": 1}, {"texto": "...", "puntaje": 2}, {"texto": "...", "puntaje": 3}, {"texto": "...", "puntaje": 4}]}], "Resources Management": [...], "Information Management": [...], "Organization Management": [...], "Innovation Management": [...]}}
                """;

        return obtenerAssistantId()
                .flatMap(assId -> crearThread()
                        .flatMap(threadId -> enviarMensaje(threadId, prompt)
                                .then(iniciarRun(assId, threadId))
                                .flatMap(runId -> esperarRun(threadId, runId)
                                        .then(obtenerRespuesta(threadId))
                                        .map(output -> ResponseEntity.ok(
                                                Map.of("output", limpiarJson(output))
                                        ))
                                )
                        )
                );
    }

    // =================== 5. Analizar Respuestas ===================
    @PostMapping("/analizarRespuestas")
    public Mono<ResponseEntity<?>> analizarRespuestas(@RequestBody Map<String, Object> body) {
        String descripcion = (String) body.get("descripcion");
        Map<String, Object> respuestas = (Map<String, Object>) body.get("respuestas");

        String prompt = """
            Empresa: """ + descripcion + """

            Respuestas: """ + formatearRespuestas(respuestas) + """

            Genera análisis IMI con puntaje (1-4) y justificación por subcategoría, recomendación por categoría, y recomendacion_general motivadora (con ejemplos de empresas y datos de digitalización).

            JSON requerido:
            {"resumen": "...", "puntajes": {"Product Management": {"Product": {"puntaje": N, "justificacion": "..."}, "Design": {...}, "Fabrication": {...}}, "Resources Management": {"Materials": {...}, "Energy": {...}, "Logistics": {...}}, "Information Management": {"Sensing": {...}, "Processing": {...}, "Actuator": {...}}, "Organization Management": {"Organization": {...}, "Impact": {...}, "Compliance": {...}}, "Innovation Management": {"Innovation": {...}, "Intellectual property": {...}, "Training": {...}}}, "recomendaciones": {"Product Management": "...", "Resources Management": "...", "Information Management": "...", "Organization Management": "...", "Innovation Management": "..."}, "recomendacion_general": "..."}
            """;

        return obtenerAssistantId()
                .flatMap(assId -> crearThread()
                        .flatMap(threadId -> enviarMensaje(threadId, prompt)
                                .then(iniciarRun(assId, threadId))
                                .flatMap(runId -> esperarRun(threadId, runId)
                                        .then(obtenerRespuesta(threadId))
                                        .map(output -> {
                                            String limpio = output.trim()
                                                    .replaceAll("^```json", "")
                                                    .replaceAll("```$", "")
                                                    .replaceAll("【[^】]+】", "");
                                            return ResponseEntity.ok(
                                                    Map.of("output", limpiarJson(limpio))
                                            );
                                        })
                                )
                        )
                );
    }


    // =================== Helpers ===================
    private Mono<String> obtenerAssistantId() {
        if (assistantId != null && !assistantId.isBlank()) {
            log.info("Reutilizando assistant existente: {}", assistantId);
            return Mono.just(assistantId);
        }
        log.warn("No hay assistant-id configurado, creando uno temporal (considera usar POST /api/imi/crear-assistant)");
        return openAiClient.post("/assistants", Map.of(
                "model", openAiModel,
                "name", "IMI Evaluador Temporal",
                "instructions", "Eres un experto en madurez industrial. Responde solo en JSON válido en español.",
                "tools", List.of(Map.of("type", "file_search")),
                "tool_resources", Map.of("file_search", Map.of("vector_store_ids", List.of(vectorStoreId)))
        )).map(resp -> {
            String id = resp.path("id").asText();
            log.info("Assistant temporal creado: {}", id);
            return id;
        });
    }

    private Mono<String> crearThread() {
        log.info("Creando thread...");
        return openAiClient.post("/threads", Map.of())
                .map(resp -> {
                    String id = resp.path("id").asText();
                    log.info("Thread creado: {}", id);
                    return id;
                });
    }

    private Mono<JsonNode> enviarMensaje(String threadId, String prompt) {
        log.info("Enviando mensaje al thread: {}", threadId);
        return openAiClient.post("/threads/" + threadId + "/messages", Map.of(
                "role", "user", "content", prompt
        )).doOnSuccess(resp -> log.info("Mensaje enviado correctamente"));
    }

    private Mono<String> iniciarRun(String assistantId, String threadId) {
        log.info("Iniciando run - assistant: {}, thread: {}, vector_store: {}", assistantId, threadId, vectorStoreId);
        return openAiClient.post("/threads/" + threadId + "/runs", Map.of(
                "assistant_id", assistantId,
                "tool_resources", Map.of("file_search",
                        Map.of("vector_store_ids", List.of(vectorStoreId)))
        )).map(resp -> {
            String id = resp.path("id").asText();
            log.info("Run iniciado: {}", id);
            return id;
        });
    }

    private Mono<JsonNode> esperarRun(String threadId, String runId) {
        log.info("Esperando completar run: {}", runId);
        return Flux.interval(Duration.ofSeconds(3))
                .flatMap(tick -> openAiClient.get("/threads/" + threadId + "/runs/" + runId))
                .doOnNext(resp -> log.info("Run status: {}", resp.path("status").asText()))
                .filter(resp -> {
                    String status = resp.path("status").asText();
                    if ("failed".equals(status) || "cancelled".equals(status) || "expired".equals(status)) {
                        log.error("Run terminó con error: {}", resp);
                        throw new RuntimeException("Run failed with status: " + status);
                    }
                    return "completed".equals(status);
                })
                .next()
                .doOnSuccess(resp -> log.info("Run completado exitosamente"));
    }

    private Mono<String> obtenerRespuesta(String threadId) {
        log.info("Obteniendo respuesta del thread: {}", threadId);
        return openAiClient.get("/threads/" + threadId + "/messages")
                .map(resp -> {
                    String value = resp.path("data").get(0).path("content").get(0).path("text").path("value").asText();
                    log.info("Respuesta obtenida, longitud: {} caracteres", value.length());
                    return value;
                });
    }

    private String formatearRespuestas(Map<String, Object> respuestas) {
        StringBuilder sb = new StringBuilder();
        respuestas.forEach((categoria, preguntas) -> {
            sb.append(categoria).append(":\n");
            if (preguntas instanceof Map<?, ?> map) {
                map.forEach((pregunta, respuesta) -> {
                    if (respuesta instanceof Map<?, ?> resMap &&
                            resMap.containsKey("texto") && resMap.containsKey("puntaje")) {
                        sb.append("- ").append(pregunta).append(": ")
                                .append(resMap.get("texto"))
                                .append(" (Puntaje: ").append(resMap.get("puntaje")).append(")\n");
                    } else if (respuesta instanceof Map<?, ?> resMap && resMap.containsKey("libre")) {
                        sb.append("- ").append(pregunta).append(" (respuesta libre): ")
                                .append(resMap.get("libre")).append("\n");
                    } else {
                        sb.append("- ").append(pregunta).append(": ").append(respuesta).append("\n");
                    }
                });
            }
            sb.append("\n");
        });
        return sb.toString();
    }

    private Map<String, Object> limpiarJson(String respuesta) {
        String limpio = respuesta.trim()
                .replaceAll("^```json\\s*", "")
                .replaceAll("\\s*```$", "")
                .replaceAll("【[^】]+】", "")
                .replaceAll("^[^{]*", "")  // Elimina todo antes del primer {
                .replaceAll("[^}]*$", "")  // Elimina todo después del último }
                .trim();

        // Asegurar que empiece con { y termine con }
        int inicio = limpio.indexOf('{');
        int fin = limpio.lastIndexOf('}');
        if (inicio >= 0 && fin > inicio) {
            limpio = limpio.substring(inicio, fin + 1);
        }

        try {
            return objectMapper.readValue(limpio, Map.class);
        } catch (Exception e) {
            log.error("Error parseando JSON: {} - Respuesta (primeros 300 chars): {}", e.getMessage(), limpio.substring(0, Math.min(300, limpio.length())));
            return Map.of("raw", limpio);
        }
    }
}
