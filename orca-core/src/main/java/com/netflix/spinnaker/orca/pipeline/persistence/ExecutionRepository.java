/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.orca.pipeline.persistence;

import static java.util.stream.Collectors.toList;

import com.netflix.spinnaker.kork.telemetry.Instrumented;
import com.netflix.spinnaker.orca.ExecutionStatus;
import com.netflix.spinnaker.orca.pipeline.model.Execution;
import com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import java.time.Instant;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import rx.Observable;

public interface ExecutionRepository {
  void store(@Nonnull Execution execution);

  void storeStage(@Nonnull Stage stage);

  void updateStageContext(@Nonnull Stage stage);

  void removeStage(@Nonnull Execution execution, @Nonnull String stageId);

  void addStage(@Nonnull Stage stage);

  /**
   * Retrieve all stages(each stage including just a few attributes: id, status, parentStageId, tasks)
   * with lightweight execution(including just a few attributes: type/id/application/lightweight/canceled/status/name/pipelineConfigId).
   *
   * @param type
   * @param id
   * @return
   * @throws ExecutionNotFoundException
   */
  @Nonnull
  Collection<Stage> retrieveAllStagesLightweight(@Nonnull ExecutionType type,
                                                 @Nonnull String id) throws ExecutionNotFoundException;

  /**
   * Retrieve stage(including all attributes) with lightweight execution(including just a few attributes:
   * type/id/application/lightweight/canceled/status/name/pipelineConfigId).
   *
   * @param type
   * @param id
   * @param stageId
   * @return
   */
  @Nonnull
  Stage retrieveStageLightweight(@Nonnull ExecutionType type,
                                 @Nonnull String id,
                                 @Nonnull String stageId);

  /**
   * Retrieve initial stages with just a few attributes:
   * <ul>
   *   <li>id</li>
   *   <li>status</li>
   *   <li>execution(type, id, application)</li>
   * </ul>
   * @param type
   * @param id
   * @return
   * @throws ExecutionNotFoundException
   */
  @Nonnull
  Collection<Stage> retrieveInitialStages(@Nonnull ExecutionType type,
                                          @Nonnull String id) throws ExecutionNotFoundException;

  /**
   * Retrieve one stage's upstream stages with just a few attributes:
   * <ul>
   *   <li>id</li>
   *   <li>status</li>
   *   <li>execution(type, id, application)</li>
   * </ul>
   * @param type
   * @param id
   * @param stageId
   * @return
   */
  @Nonnull
  Collection<Stage> retrieveUpstreamStages(@Nonnull ExecutionType type,
                                           @Nonnull String id,
                                           @Nonnull String stageId);

  /**
   * Retrieve one stage's downstream stages with just a few attributes:
   * <ul>
   *   <li>id</li>
   *   <li>status</li>
   *   <li>execution(type, id, application)</li>
   * </ul>
   * @param type
   * @param id
   * @param stageId
   * @return
   */
  @Nonnull
  Collection<Stage> retrieveDownstreamStages(@Nonnull ExecutionType type,
                                             @Nonnull String id,
                                             @Nonnull String stageId);

  @Instrumented(metricName = "cancelNullReason")
  void cancel(@Nonnull ExecutionType type, @Nonnull String id);

  void cancel(
      @Nonnull ExecutionType type,
      @Nonnull String id,
      @Nullable String user,
      @Nullable String reason);

  void pause(@Nonnull ExecutionType type, @Nonnull String id, @Nullable String user);

  @Instrumented(metricName = "resumeNoIgnore")
  void resume(@Nonnull ExecutionType type, @Nonnull String id, @Nullable String user);

  void resume(
      @Nonnull ExecutionType type,
      @Nonnull String id,
      @Nullable String user,
      boolean ignoreCurrentStatus);

  boolean isCanceled(ExecutionType type, @Nonnull String id);

  void updateStatus(ExecutionType type, @Nonnull String id, @Nonnull ExecutionStatus status);

  @Nonnull
  @Instrumented(metricName = "retrieveById")
  Execution retrieve(@Nonnull ExecutionType type, @Nonnull String id)
      throws ExecutionNotFoundException;

  /**
   * Retrieve execution with just a few attributes(type/id/application/lightweight/canceled/status/name/pipelineConfigId), without stages.
   *
   * @param type see {@link ExecutionType}
   * @param id see {@link Execution#getId()}
   * @return
   * @throws ExecutionNotFoundException
   */
  @Nonnull
  @Instrumented(metricName = "retrieveLightweightById")
  Execution retrieveLightweight(@Nonnull ExecutionType type, @Nonnull String id)
      throws ExecutionNotFoundException;

  void delete(@Nonnull ExecutionType type, @Nonnull String id);

  @Nonnull
  @Instrumented(metricName = "retrieveByType")
  Observable<Execution> retrieve(@Nonnull ExecutionType type);

  @Nonnull
  @Instrumented(metricName = "retrieveByCriteria")
  Observable<Execution> retrieve(@Nonnull ExecutionType type, @Nonnull ExecutionCriteria criteria);

  @Nonnull
  Observable<Execution> retrievePipelinesForApplication(@Nonnull String application);

  @Nonnull
  Observable<Execution> retrievePipelinesForPipelineConfigId(
      @Nonnull String pipelineConfigId, @Nonnull ExecutionCriteria criteria);

  /**
   * Retrieve a pipeline's lightweight executions, each execution with just a few attributes, without stages.
   *
   * @param pipelineConfigId
   * @param criteria
   * @return
   */
  @Nonnull
  Observable<Execution> retrievePipelinesLightweightForPipelineConfigId(
      @Nonnull String pipelineConfigId, @Nonnull ExecutionCriteria criteria);

  /**
   * Returns executions in the time boundary. Redis impl does not respect pageSize or offset params,
   * and returns all executions. Sql impl respects these params.
   *
   * @param executionCriteria use this param to specify: if there are statuses, only those will be
   *     returned if there is a sort type that will be used to sort the results use pageSize and
   *     page to control pagination
   */
  @Nonnull
  List<Execution> retrievePipelinesForPipelineConfigIdsBetweenBuildTimeBoundary(
      @Nonnull List<String> pipelineConfigIds,
      long buildTimeStartBoundary,
      long buildTimeEndBoundary,
      ExecutionCriteria executionCriteria);

  /**
   * Returns all executions in the time boundary
   *
   * @param executionCriteria if there are statuses, only those will be returned if there is a
   *     pageSize, that will be used as the page size if there is a sort type that will be used to
   *     sort the results
   */
  @Nonnull
  List<Execution> retrieveAllPipelinesForPipelineConfigIdsBetweenBuildTimeBoundary(
      @Nonnull List<String> pipelineConfigIds,
      long buildTimeStartBoundary,
      long buildTimeEndBoundary,
      ExecutionCriteria executionCriteria);

  @Deprecated // Use the non-rx interface instead
  @Nonnull
  @Instrumented(metricName = "retrieveOrchestrationsForApplicationSortedAsc")
  Observable<Execution> retrieveOrchestrationsForApplication(
      @Nonnull String application, @Nonnull ExecutionCriteria criteria);

  @Nonnull
  List<Execution> retrieveOrchestrationsForApplication(
      @Nonnull String application,
      @Nonnull ExecutionCriteria criteria,
      @Nullable ExecutionComparator sorter);

  @Nonnull
  Execution retrieveByCorrelationId(
      @Nonnull ExecutionType executionType, @Nonnull String correlationId)
      throws ExecutionNotFoundException;

  @Deprecated
  @Nonnull
  Execution retrieveOrchestrationForCorrelationId(@Nonnull String correlationId)
      throws ExecutionNotFoundException;

  @Deprecated
  @Nonnull
  Execution retrievePipelineForCorrelationId(@Nonnull String correlationId)
      throws ExecutionNotFoundException;

  @Nonnull
  List<Execution> retrieveBufferedExecutions();

  @Nonnull
  List<String> retrieveAllApplicationNames(@Nullable ExecutionType executionType);

  @Nonnull
  @Instrumented(metricName = "retrieveAllApplicationNamesWithMinExecutions")
  List<String> retrieveAllApplicationNames(
      @Nullable ExecutionType executionType, int minExecutions);

  boolean hasExecution(@Nonnull ExecutionType type, @Nonnull String id);

  List<String> retrieveAllExecutionIds(@Nonnull ExecutionType type);

  /**
   * Returns the name of the partition that this execution repository owns/handles. {@code null}
   * means that it handles any partition (in other words, this execution repository is not partition
   * aware)
   */
  @Nullable
  default String getPartition() {
    return null;
  }

  default boolean handlesPartition(@Nullable String partitionOfExecution) {
    return partitionOfExecution
            == null // executions with no partition are implied to be owned by current orca
        || getPartition()
            == null // this repository is not restricted to a partition, can handle any execution
        || partitionOfExecution.equals(getPartition()); // both are set and must match
  }

  final class ExecutionCriteria {
    private int pageSize = 3500;
    private Collection<ExecutionStatus> statuses = new ArrayList<>();
    private int page;
    private Instant startTimeCutoff;
    private ExecutionComparator sortType;

    public int getPageSize() {
      return pageSize;
    }

    public @Nonnull ExecutionCriteria setPageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public @Nonnull Collection<ExecutionStatus> getStatuses() {
      return statuses;
    }

    public @Nonnull ExecutionCriteria setStatuses(Collection<String> statuses) {
      return setStatuses(
          statuses.stream()
              .map(it -> ExecutionStatus.valueOf(it.toUpperCase()))
              .collect(toList())
              .toArray(new ExecutionStatus[statuses.size()]));
    }

    public @Nonnull ExecutionCriteria setStatuses(ExecutionStatus... statuses) {
      this.statuses = Arrays.asList(statuses);
      return this;
    }

    public int getPage() {
      return Math.max(page, 1);
    }

    public ExecutionCriteria setPage(int page) {
      this.page = page;
      return this;
    }

    public @Nullable Instant getStartTimeCutoff() {
      return startTimeCutoff;
    }

    public ExecutionCriteria setStartTimeCutoff(Instant startTimeCutoff) {
      this.startTimeCutoff = startTimeCutoff;
      return this;
    }

    public ExecutionComparator getSortType() {
      return sortType;
    }

    public ExecutionCriteria setSortType(ExecutionComparator sortType) {
      this.sortType = sortType;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ExecutionCriteria that = (ExecutionCriteria) o;
      return pageSize == that.pageSize
          && Objects.equals(statuses, that.statuses)
          && page == that.page;
    }

    @Override
    public int hashCode() {
      return Objects.hash(pageSize, statuses, page);
    }
  }

  enum ExecutionComparator implements Comparator<Execution> {
    NATURAL_ASC {
      @Override
      public int compare(Execution a, Execution b) {
        return b.getId().compareTo(a.getId());
      }
    },

    NATURAL_DESC {
      @Override
      public int compare(Execution a, Execution b) {
        return a.getId().compareTo(b.getId());
      }
    },

    /**
     * Sort executions nulls first, then by startTime descending, breaking ties by lexicographically
     * descending IDs.
     */
    START_TIME_OR_ID {
      @Override
      public int compare(Execution a, Execution b) {
        Long aStartTime = a.getStartTime();
        Long bStartTime = b.getStartTime();

        int startCompare;
        if (aStartTime == null) {
          startCompare = bStartTime == null ? 0 : -1;
        } else if (bStartTime == null) {
          startCompare = 1;
        } else {
          startCompare = bStartTime.compareTo(aStartTime);
        }

        if (startCompare == 0) {
          return b.getId().compareTo(a.getId());
        }
        return startCompare;
      }
    },

    BUILD_TIME_DESC {
      @Override
      public int compare(Execution a, Execution b) {
        Long aBuildTime = Optional.ofNullable(a.getBuildTime()).orElse(0L);
        Long bBuildTime = Optional.ofNullable(b.getBuildTime()).orElse(0L);

        int buildCompare = bBuildTime.compareTo(aBuildTime);
        if (buildCompare == 0) {
          return b.getId().compareTo(a.getId());
        }
        return buildCompare;
      }
    },

    BUILD_TIME_ASC {
      @Override
      public int compare(Execution a, Execution b) {
        Long aBuildTime = Optional.ofNullable(a.getBuildTime()).orElse(0L);
        Long bBuildTime = Optional.ofNullable(b.getBuildTime()).orElse(0L);

        int buildCompare = aBuildTime.compareTo(bBuildTime);
        if (buildCompare == 0) {
          return a.getId().compareTo(b.getId());
        }
        return buildCompare;
      }
    };
  }
}
