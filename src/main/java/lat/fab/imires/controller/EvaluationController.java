package lat.fab.imires.controller;

import lat.fab.imires.model.AiEvaluation;
import lat.fab.imires.model.AiEvaluationSummary;
import lat.fab.imires.repository.DataAiEvaluationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@Log4j2
public class EvaluationController {

    private final DataAiEvaluationRepository evaluationRepository;

    // Persist one AI evaluation iteration (called by the frontend after the analysis completes).
    @PostMapping("/evaluations")
    public Mono<AiEvaluation> save(@RequestBody AiEvaluation evaluation) {
        evaluation.setId(null);
        evaluation.setCreatedAt(new Date());
        return evaluationRepository.save(evaluation);
    }

    // User satisfaction feedback (1-5) for an existing evaluation.
    @PutMapping("/evaluations/{id}/feedback")
    public Mono<AiEvaluation> saveFeedback(@PathVariable String id, @RequestBody Map<String, Integer> body) {
        Integer feedback = body.get("feedback");
        return evaluationRepository.findById(id)
                .flatMap(e -> {
                    e.setFeedback(feedback);
                    return evaluationRepository.save(e);
                });
    }

    // Admin table view: lightweight list, newest first.
    @GetMapping("/admin/evaluations")
    public Flux<AiEvaluationSummary> list() {
        return evaluationRepository.findAllByOrderByCreatedAtDesc()
                .map(e -> new AiEvaluationSummary(e.getId(), e.getCompanyName(),
                        e.getUserEmail(), e.getUserType(), e.getFeedback(), e.getCreatedAt()));
    }

    // Admin detail view: full stored iteration.
    @GetMapping("/admin/evaluations/{id}")
    public Mono<AiEvaluation> detail(@PathVariable String id) {
        return evaluationRepository.findById(id);
    }
}
