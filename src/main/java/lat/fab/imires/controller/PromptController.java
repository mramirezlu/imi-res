package lat.fab.imires.controller;

import lat.fab.imires.model.PromptConfig;
import lat.fab.imires.repository.DataPromptRepository;
import lat.fab.imires.service.PromptService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class PromptController {

    private final PromptService promptService;
    private final DataPromptRepository promptRepository;

    // Admin table: list of the editable AI prompts.
    @GetMapping("/admin/prompts")
    public Flux<Map<String, Object>> list() {
        return Flux.fromIterable(promptService.keys())
                .flatMap(key -> promptRepository.findById(key)
                        .map(pc -> resumen(key, pc.getUpdatedAt(), true))
                        .defaultIfEmpty(resumen(key, null, false)));
    }

    // Admin detail: editable text + read-only fixed parts (variables and JSON format).
    @GetMapping("/admin/prompts/{key}")
    public Mono<Map<String, Object>> detail(@PathVariable String key) {
        if (!promptService.isValidKey(key)) return Mono.empty();
        return promptRepository.findById(key)
                .map(pc -> {
                    String texto = (pc.getTextoEditable() != null && !pc.getTextoEditable().isBlank())
                            ? pc.getTextoEditable() : promptService.defaultText(key);
                    return detalle(key, texto, pc.getUpdatedAt(), true);
                })
                .defaultIfEmpty(detalle(key, promptService.defaultText(key), null, false));
    }

    // Update the editable text of a prompt.
    @PutMapping("/admin/prompts/{key}")
    public Mono<PromptConfig> update(@PathVariable String key, @RequestBody Map<String, String> body) {
        String texto = body.getOrDefault("textoEditable", "");
        return promptRepository.save(new PromptConfig(key, texto, new Date()));
    }

    // Reset a prompt to its default (removes the stored override).
    @PostMapping("/admin/prompts/{key}/reset")
    public Mono<Void> reset(@PathVariable String key) {
        return promptRepository.deleteById(key);
    }

    private Map<String, Object> resumen(String key, Date updatedAt, boolean personalizado) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        m.put("nombre", promptService.nombre(key));
        m.put("descripcion", promptService.descripcion(key));
        m.put("updatedAt", updatedAt);
        m.put("personalizado", personalizado);
        return m;
    }

    private Map<String, Object> detalle(String key, String texto, Date updatedAt, boolean personalizado) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        m.put("nombre", promptService.nombre(key));
        m.put("descripcion", promptService.descripcion(key));
        m.put("textoEditable", texto);
        m.put("defaultText", promptService.defaultText(key));
        m.put("variablesInfo", promptService.variablesInfo(key));
        m.put("formatoInfo", promptService.formatoInfo(key));
        m.put("updatedAt", updatedAt);
        m.put("personalizado", personalizado);
        return m;
    }
}
