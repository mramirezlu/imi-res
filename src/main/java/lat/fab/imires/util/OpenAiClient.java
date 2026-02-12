package lat.fab.imires.util;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
@Slf4j
public class OpenAiClient {

    private final WebClient client;
    private final String apiKeyMasked;

    public OpenAiClient(@Value("${openai.api.key}") String apiKey) {
        this.client = WebClient.builder()
                .baseUrl("https://api.openai.com/v1")
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .defaultHeader("OpenAI-Beta", "assistants=v2")
                .build();
        // Guardar versión enmascarada (solo últimos 8 caracteres)
        this.apiKeyMasked = "sk-......" + apiKey.substring(Math.max(0, apiKey.length() - 8));
    }

    public String getApiKeyMasked() {
        return apiKeyMasked;
    }

    public Mono<JsonNode> post(String path, Object body) {
        return client.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                    response.bodyToMono(String.class)
                        .flatMap(errorBody -> {
                            log.error("OpenAI POST {} error: {} - {}", path, response.statusCode(), errorBody);
                            return Mono.error(new RuntimeException("OpenAI error: " + errorBody));
                        }))
                .bodyToMono(JsonNode.class)
                .doOnSuccess(resp -> log.debug("OpenAI POST {} success", path));
    }

    public Mono<JsonNode> get(String path) {
        return client.get()
                .uri(path)
                .retrieve()
                .onStatus(status -> status.isError(), response ->
                    response.bodyToMono(String.class)
                        .flatMap(errorBody -> {
                            log.error("OpenAI GET {} error: {} - {}", path, response.statusCode(), errorBody);
                            return Mono.error(new RuntimeException("OpenAI error: " + errorBody));
                        }))
                .bodyToMono(JsonNode.class);
    }

    public Mono<JsonNode> multipart(String path, byte[] contenido, String nombreArchivo, Map<String, String> extraFields) {
        ByteArrayResource fileResource = new ByteArrayResource(contenido) {
            @Override
            public String getFilename() {
                return nombreArchivo;
            }
        };

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", fileResource);
        if (extraFields != null) {
            extraFields.forEach(body::add);
        }

        return client.post()
                .uri(path)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(JsonNode.class);
    }
}

