/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.pipeline;

import static com.netflix.spinnaker.orca.pipeline.model.Execution.AuthenticationDetails;
import static com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType;
import static com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType.PIPELINE;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.kork.web.exceptions.ValidationException;
import com.netflix.spinnaker.orca.ExecutionStatus;
import com.netflix.spinnaker.orca.events.BeforeInitialExecutionPersist;
import com.netflix.spinnaker.orca.pipeline.model.Execution;
import com.netflix.spinnaker.orca.pipeline.model.PipelineBuilder;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import com.netflix.spinnaker.orca.pipeline.model.Trigger;
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionNotFoundException;
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository;
import java.io.IOException;
import java.io.Serializable;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class ExecutionLauncher {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final ObjectMapper objectMapper;
  private final ExecutionRepository executionRepository;
  private final ExecutionRunner executionRunner;
  private final Clock clock;
  private final Optional<PipelineValidator> pipelineValidator;
  private final Optional<Registry> registry;
  private final ApplicationEventPublisher applicationEventPublisher;

  @Autowired
  public ExecutionLauncher(
      ObjectMapper objectMapper,
      ExecutionRepository executionRepository,
      ExecutionRunner executionRunner,
      Clock clock,
      ApplicationEventPublisher applicationEventPublisher,
      Optional<PipelineValidator> pipelineValidator,
      Optional<Registry> registry) {
    this.objectMapper = objectMapper;
    this.executionRepository = executionRepository;
    this.executionRunner = executionRunner;
    this.clock = clock;
    this.applicationEventPublisher = applicationEventPublisher;
    this.pipelineValidator = pipelineValidator;
    this.registry = registry;
  }

  /**
   * Start one execution according to pipeline configJson.<p/>
   * Sample configJson value when create/save pipeline:
   * <pre>
   * {
   *   "application": "ycc",
   *   "name": "Save pipeline 'test'",
   *   "stages": [{
   *     "type": "savePipeline",
   *     "pipeline": "eyJhcHBsaWNhdGlvbiI6InljYyIsImluZGV4IjoxOSwia2VlcFdhaXRpbmdQaXBlbGluZXMiOmZhbHNlLCJsaW1pdENvbmN1cnJlbnQiOnRydWUsIm5hbWUiOiJ0ZXN0Iiwic3BlbEV2YWx1YXRvciI6InY0Iiwic3RhZ2VzIjpbXSwidHJpZ2dlcnMiOltdfQ==",
   *     "user": "anonymous",
   *     "refId": "0",
   *     "requisiteStageRefIds": []
   *   }],
   *   "trigger": {
   *     "type": "manual",
   *     "user": "anonymous",
   *     "parameters": {},
   *     "artifacts": [],
   *     "expectedArtifacts": [],
   *     "resolvedExpectedArtifacts": []
   *   },
   *   "origin": "api"
   * }
   * </pre>
   *
   * @param type
   * @param configJson
   * @return
   * @throws Exception
   */
  public Execution start(ExecutionType type, String configJson) throws Exception {
    final Execution execution = parse(type, configJson);

    final Execution existingExecution = checkForCorrelatedExecution(execution);
    if (existingExecution != null) {
      return existingExecution;
    }

    checkRunnable(execution);

    persistExecution(execution);

    try {
      start(execution);
    } catch (Throwable t) {
      handleStartupFailure(execution, t);
    }

    return execution;
  }

  /**
   * Log that an execution failed; useful if a pipeline failed validation and we want to persist the
   * failure to the execution history but don't actually want to attempt to run the execution.
   *
   * @param e the exception that was thrown during pipeline validation
   */
  public Execution fail(ExecutionType type, String configJson, Exception e) throws Exception {
    final Execution execution = parse(type, configJson);

    persistExecution(execution);

    handleStartupFailure(execution, e);

    return execution;
  }

  private void checkRunnable(Execution execution) {
    if (execution.getType() == PIPELINE) {
      pipelineValidator.ifPresent(it -> it.checkRunnable(execution));
    }
  }

  public Execution start(Execution execution) throws Exception {
    executionRunner.start(execution);
    return execution;
  }

  private Execution checkForCorrelatedExecution(Execution execution) {
    if (execution.getTrigger().getCorrelationId() == null) {
      return null;
    }

    Trigger trigger = execution.getTrigger();

    try {
      Execution o =
          executionRepository.retrieveByCorrelationId(
              execution.getType(), trigger.getCorrelationId());
      log.info(
          "Found pre-existing "
              + execution.getType()
              + " by correlation id (id: "
              + o.getId()
              + ", correlationId: "
              + trigger.getCorrelationId()
              + ")");
      return o;
    } catch (ExecutionNotFoundException e) {
      // Swallow
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  private Execution handleStartupFailure(Execution execution, Throwable failure) {
    final String canceledBy = "system";
    String reason = "Failed on startup: " + failure.getMessage();
    final ExecutionStatus status = ExecutionStatus.TERMINAL;

    if (failure instanceof ValidationException) {
      ValidationException validationException = (ValidationException) failure;
      if (validationException.getAdditionalAttributes().containsKey("errors")) {
        List<Map<String, Object>> errors =
            ((List<Map<String, Object>>)
                validationException.getAdditionalAttributes().get("errors"));
        reason +=
            errors.stream()
                .flatMap(
                    error ->
                        error.entrySet().stream()
                            .filter(entry -> !entry.getKey().equals("severity")))
                .map(
                    entry ->
                        "\n" + WordUtils.capitalizeFully(entry.getKey()) + ": " + entry.getValue())
                .collect(Collectors.joining("\n", "\n", ""));
      }
    }

    log.error("Failed to start {} {}", execution.getType(), execution.getId(), failure);
    executionRepository.updateStatus(execution.getType(), execution.getId(), status);
    executionRepository.cancel(execution.getType(), execution.getId(), canceledBy, reason);
    return executionRepository.retrieve(execution.getType(), execution.getId());
  }

  /**
   * Create one Execution object to execute.
   *
   * @param type
   * @param configJson
   * @return Sample json when create/save pipeline(go into parseOrchestration):
   * <pre>
   * {<br/>
   *   "type": "ORCHESTRATION",<br/>
   *   "id": "01EYMJ4RP3SC7YB9E9FBQRMQG8",<br/>
   *   "application": "ycc",<br/>
   *   "name": null,<br/>
   *   "buildTime": 1613450339029,<br/>
   *   "canceled": false,<br/>
   *   "canceledBy": null,<br/>
   *   "cancellationReason": null,<br/>
   *   "limitConcurrent": false,<br/>
   *   "keepWaitingPipelines": false,<br/>
   *   "stages": [{<br/>
   *     "id": "01EYMJ4RP48G4GJ70WGJDG5FQD",<br/>
   *     "refId": "0",<br/>
   *     "type": "savePipeline",<br/>
   *     "name": "savePipeline",<br/>
   *     "startTime": null,<br/>
   *     "endTime": null,<br/>
   *     "startTimeExpiry": null,<br/>
   *     "status": "NOT_STARTED",<br/>
   *     "context": {<br/>
   *       "pipeline": "eyJhcHBsaWNhdGlvbiI6InljYyIsImluZGV4IjoxOSwia2VlcFdhaXRpbmdQaXBlbGluZXMiOmZhbHNlLCJsaW1pdENvbmN1cnJlbnQiOnRydWUsIm5hbWUiOiJ0ZXN0Iiwic3BlbEV2YWx1YXRvciI6InY0Iiwic3RhZ2VzIjpbXSwidHJpZ2dlcnMiOltdfQ==",<br/>
   *       "user": "anonymous"<br/>
   *     },<br/>
   *     "outputs": {},<br/>
   *     "tasks": [],<br/>
   *     "syntheticStageOwner": null,<br/>
   *     "parentStageId": null,<br/>
   *     "requisiteStageRefIds": [],<br/>
   *     "scheduledTime": null,<br/>
   *     "lastModified": null<br/>
   *   }],<br/>
   *   "startTime": null,<br/>
   *   "endTime": null,<br/>
   *   "startTimeExpiry": null,<br/>
   *   "status": "NOT_STARTED",<br/>
   *   "authentication": {<br/>
   *     "user": "anonymous",<br/>
   *     "allowedAccounts": []<br/>
   *   },<br/>
   *   "paused": null,<br/>
   *   "origin": "api",<br/>
   *   "trigger": {<br/>
   *     "type": "manual",<br/>
   *     "correlationId": null,<br/>
   *     "notifications": [],<br/>
   *     "strategy": false,<br/>
   *     "rebake": false,<br/>
   *     "dryRun": false,<br/>
   *     "user": "anonymous",<br/>
   *     "parameters": {},<br/>
   *     "artifacts": [],<br/>
   *     "expectedArtifacts": [],<br/>
   *     "resolvedExpectedArtifacts": []<br/>
   *   },<br/>
   *   "description": "Save pipeline 'test'",<br/>
   *   "pipelineConfigId": null,<br/>
   *   "source": null,<br/>
   *   "notifications": [],<br/>
   *   "initialConfig": {},<br/>
   *   "systemNotifications": [],<br/>
   *   "spelEvaluator": null,<br/>
   *   "templateVariables": null,<br/>
   *   "partition": null<br/>
   * }
   * </pre>
   * @throws IOException
   */
  private Execution parse(ExecutionType type, String configJson) throws IOException {
    if (type == PIPELINE) {
      return parsePipeline(configJson);
    } else {
      return parseOrchestration(configJson);
    }
  }

  private Execution parsePipeline(String configJson) throws IOException {
    // TODO: can we not just annotate the class properly to avoid all this?
    Map<String, Serializable> config = objectMapper.readValue(configJson, Map.class);
    return new PipelineBuilder(getString(config, "application"))
        .withId(getString(config, "executionId"))
        .withName(getString(config, "name"))
        .withPipelineConfigId(getString(config, "id"))
        .withTrigger(objectMapper.convertValue(config.get("trigger"), Trigger.class))
        .withStages((List<Map<String, Object>>) config.get("stages"))
        .withLimitConcurrent(getBoolean(config, "limitConcurrent"))
        .withKeepWaitingPipelines(getBoolean(config, "keepWaitingPipelines"))
        .withNotifications((List<Map<String, Object>>) config.get("notifications"))
        .withInitialConfig((Map<String, Object>) config.get("initialConfig"))
        .withOrigin(getString(config, "origin"))
        .withStartTimeExpiry(getString(config, "startTimeExpiry"))
        .withSource(
            (config.get("source") == null)
                ? null
                : objectMapper.convertValue(config.get("source"), Execution.PipelineSource.class))
        .withSpelEvaluator(getString(config, "spelEvaluator"))
        .withTemplateVariables((Map<String, Object>) config.get("templateVariables"))
        .build();
  }

  /**
   * Create Execution according to pipeline configJson, then initialize its fields.<p/>
   * Sample configJson when create/save pipeline:
   * <pre>
   * {
   *   "application": "ycc",
   *   "name": "Save pipeline 'test'",
   *   "stages": [{
   *     "type": "savePipeline",
   *     "pipeline": "eyJhcHBsaWNhdGlvbiI6InljYyIsImluZGV4IjoxOSwia2VlcFdhaXRpbmdQaXBlbGluZXMiOmZhbHNlLCJsaW1pdENvbmN1cnJlbnQiOnRydWUsIm5hbWUiOiJ0ZXN0Iiwic3BlbEV2YWx1YXRvciI6InY0Iiwic3RhZ2VzIjpbXSwidHJpZ2dlcnMiOltdfQ==",
   *     "user": "anonymous",
   *     "refId": "0",
   *     "requisiteStageRefIds": []
   *   }],
   *   "trigger": {
   *     "type": "manual",
   *     "user": "anonymous",
   *     "parameters": {},
   *     "artifacts": [],
   *     "expectedArtifacts": [],
   *     "resolvedExpectedArtifacts": []
   *   },
   *   "origin": "api"
   * }
   * </pre>
   *
   * @param configJson
   * @return
   * @throws IOException
   */
  private Execution parseOrchestration(String configJson) throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, Serializable> config = objectMapper.readValue(configJson, Map.class);
    /*
    When orchestration created, three fields will be initialized:
      type=ExecutionType.ORCHESTRATION
      id=ID_GENERATOR.nextULID()
      application=config's application
     */
    Execution orchestration = Execution.newOrchestration(getString(config, "application"));
    // initialize orchestration's description field
    if (config.containsKey("name")) {
      orchestration.setDescription(getString(config, "name"));
    }
    if (config.containsKey("description")) {
      orchestration.setDescription(getString(config, "description"));
    }

    // initialize orchestration's stages field
    /*
    "stages": [{
      "type": "savePipeline",
      "pipeline": "eyJhcHBsaWNhdGlvbiI6InljYyIsImluZGV4IjoxOSwia2VlcFdhaXRpbmdQaXBlbGluZXMiOmZhbHNlLCJsaW1pdENvbmN1cnJlbnQiOnRydWUsIm5hbWUiOiJ0ZXN0Iiwic3BlbEV2YWx1YXRvciI6InY0Iiwic3RhZ2VzIjpbXSwidHJpZ2dlcnMiOltdfQ==",
      "user": "anonymous",
      "refId": "0",
      "requisiteStageRefIds": []
    }]
     */
    for (Map<String, Object> context : getList(config, "stages")) {
      String type = context.remove("type").toString();//i.e. savePipeline

      String providerType = getString(context, "providerType");
      if (providerType != null && !providerType.equals("aws") && !providerType.equals("titus")) {
        type += format("_%s", providerType);
      }

      // TODO: need to check it's valid?
      Stage stage = new Stage(orchestration, type, context);
      orchestration.getStages().add(stage);
    }

    // initialize orchestration's trigger and notifications field
    /*
    "trigger": {
      "type": "manual",
      "user": "anonymous",
      "parameters": {},
      "artifacts": [],
      "expectedArtifacts": [],
      "resolvedExpectedArtifacts": []
    }
     */
    if (config.get("trigger") != null) {
      // DefaultTrigger when type is manual
      Trigger trigger = objectMapper.convertValue(config.get("trigger"), Trigger.class);
      orchestration.setTrigger(trigger);
      if (!trigger.getNotifications().isEmpty()) {
        orchestration.setNotifications(trigger.getNotifications());
      }
    }

    orchestration.setBuildTime(clock.millis());
    orchestration.setAuthentication(
        AuthenticationDetails.build().orElse(new AuthenticationDetails()));
    orchestration.setOrigin((String) config.getOrDefault("origin", "unknown"));
    orchestration.setStartTimeExpiry((Long) config.get("startTimeExpiry"));
    orchestration.setSpelEvaluator(getString(config, "spelEvaluator"));

    return orchestration;
  }

  /** Persist the initial execution configuration. */
  private void persistExecution(Execution execution) {
    applicationEventPublisher.publishEvent(new BeforeInitialExecutionPersist(this, execution));
    executionRepository.store(execution);
  }

  private final boolean getBoolean(Map<String, ?> map, String key) {
    return parseBoolean(getString(map, key));
  }

  /**
   * Get key's value from map. If there's no key, return null
   *
   * @param map map data
   * @param key the key you want to get its value from the map
   * @return key's value or null
   */
  private final String getString(Map<String, ?> map, String key) {
    return map.containsKey(key) ? map.get(key).toString() : null;
  }

  private final <K, V> Map<K, V> getMap(Map<String, ?> map, String key) {
    Map<K, V> result = (Map<K, V>) map.get(key);
    return result == null ? emptyMap() : result;
  }

  /**
   * Get key's value from map, its value should be List<Map<String, Object>>.
   * If there's no key, return emptyList().
   *
   * @param map map data
   * @param key the key you want to get its value from the map
   * @return key's value or emptyList()
   */
  private final List<Map<String, Object>> getList(Map<String, ?> map, String key) {
    List<Map<String, Object>> result = (List<Map<String, Object>>) map.get(key);
    return result == null ? emptyList() : result;
  }

  private final <E extends Enum<E>> E getEnum(Map<String, ?> map, String key, Class<E> type) {
    String value = (String) map.get(key);
    return value != null ? Enum.valueOf(type, value) : null;
  }
}
