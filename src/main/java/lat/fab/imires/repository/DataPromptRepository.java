package lat.fab.imires.repository;

import lat.fab.imires.model.PromptConfig;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataPromptRepository extends ReactiveMongoRepository<PromptConfig, String> {
}
