package lat.fab.imires.repository;

import lat.fab.imires.model.AiEvaluation;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface DataAiEvaluationRepository extends ReactiveMongoRepository<AiEvaluation, String> {

    Flux<AiEvaluation> findAllByOrderByCreatedAtDesc();

    // best-rated evaluations first (ties broken by most recent); used as few-shot context
    Flux<AiEvaluation> findByFeedbackNotNullOrderByFeedbackDescCreatedAtDesc();
}
