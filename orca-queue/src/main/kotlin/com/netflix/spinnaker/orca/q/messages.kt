/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.spinnaker.orca.q

import com.fasterxml.jackson.annotation.JsonTypeName
import com.netflix.spinnaker.orca.ExecutionStatus
import com.netflix.spinnaker.orca.Task
import com.netflix.spinnaker.orca.pipeline.model.Execution
import com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner
import com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.q.Attribute
import com.netflix.spinnaker.q.Message
import java.time.Duration

/**
 * Messages used internally by the queueing system.
 */
interface ApplicationAware {
  val application: String
}

interface ExecutionLevel : ApplicationAware {
  val executionType: ExecutionType
  val executionId: String
  /**
   * true: just retrieve lightweight execution.
   * false: retrieve execution normally.
   */
  val lightweight: Boolean
}

interface StageLevel : ExecutionLevel {
  val stageId: String
}

interface TaskLevel : StageLevel {
  val taskId: String
}

@JsonTypeName("startTask")
data class StartTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  constructor(source: ExecutionLevel, stageId: String, taskId: String, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, stageId, taskId, lightweight)

  constructor(source: StageLevel, taskId: String, lightweight: Boolean = false) :
    this(source, source.stageId, taskId, lightweight)

  constructor(source: Stage, taskId: String, lightweight: Boolean = false) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, taskId, lightweight)

  constructor(source: Stage, task: com.netflix.spinnaker.orca.pipeline.model.Task) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, task.id)
}

@JsonTypeName("completeTask")
data class CompleteTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  val status: ExecutionStatus,
  val originalStatus: ExecutionStatus?,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  constructor(source: TaskLevel, status: ExecutionStatus, lightweight: Boolean = false) :
    this(source, status, status, lightweight)

  constructor(source: TaskLevel, status: ExecutionStatus, originalStatus: ExecutionStatus, lightweight: Boolean = false) :
    this(
      source.executionType,
      source.executionId,
      source.application,
      source.stageId,
      source.taskId,
      status,
      originalStatus,
      lightweight
    )
}

@JsonTypeName("pauseTask")
data class PauseTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  constructor(message: TaskLevel, lightweight: Boolean = false) :
    this(message.executionType, message.executionId, message.application, message.stageId, message.taskId, lightweight)
}

@JsonTypeName("resumeTask")
data class ResumeTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  constructor(message: StageLevel, taskId: String) :
    this(message.executionType, message.executionId, message.application, message.stageId, taskId, message.lightweight)
}

@JsonTypeName("restartTask")
data class RestartTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  val user: String?,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  constructor(source: Execution, stageId: String, taskId: String, user: String?) :
    this(source.type, source.id, source.application, stageId, taskId, user, source.isLightweight)
}

@JsonTypeName("runTask")
data class RunTask(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  val taskType: Class<out Task>,
  override val lightweight: Boolean = false
) : Message(), TaskLevel {
  override val ackTimeoutMs = Duration.ofMinutes(10).toMillis()

  constructor(message: StageLevel, taskId: String, taskType: Class<out Task>) :
    this(message.executionType, message.executionId, message.application, message.stageId, taskId, taskType, message.lightweight)

  constructor(message: TaskLevel, taskType: Class<out Task>) :
    this(message.executionType, message.executionId, message.application, message.stageId, message.taskId, taskType, message.lightweight)

  constructor(source: ExecutionLevel, stageId: String, taskId: String, taskType: Class<out Task>) :
    this(source.executionType, source.executionId, source.application, stageId, taskId, taskType, source.lightweight)
}

@JsonTypeName("startStage")
data class StartStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: ExecutionLevel, stageId: String) :
    this(source.executionType, source.executionId, source.application, stageId, source.lightweight)

  constructor(source: StageLevel) :
    this(source, source.stageId)

  constructor(source: Stage, lightweight: Boolean = false) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, lightweight)
}

@JsonTypeName("continueParentStage")
data class ContinueParentStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  /**
   * The phase that just completed, either before or after stages.
   */
  val phase: SyntheticStageOwner = STAGE_BEFORE,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: StageLevel, phase: SyntheticStageOwner) :
    this(source.executionType, source.executionId, source.application, source.stageId, phase)

  constructor(source: Stage, phase: SyntheticStageOwner) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, phase)
}

@JsonTypeName("completeStage")
data class CompleteStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: ExecutionLevel, stageId: String) :
    this(source.executionType, source.executionId, source.application, stageId)

  constructor(source: StageLevel, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, source.stageId, lightweight)

  constructor(source: Stage, lightweight: Boolean = false) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, lightweight)
}

@JsonTypeName("skipStage")
data class SkipStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: StageLevel) :
    this(source.executionType, source.executionId, source.application, source.stageId)

  constructor(source: Stage, lightweight: Boolean = false) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, lightweight)
}

@JsonTypeName("abortStage")
data class AbortStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: StageLevel) :
    this(source.executionType, source.executionId, source.application, source.stageId)

  constructor(source: Stage) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id)
}

@JsonTypeName("pauseStage")
data class PauseStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: StageLevel, lightweight: Boolean = false) :
    this(source, source.stageId, lightweight)

  constructor(source: ExecutionLevel, stageId: String, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, stageId, lightweight)
}

@JsonTypeName("restartStage")
data class RestartStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  val user: String?,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: Execution, stageId: String, user: String?) :
    this(source.type, source.id, source.application, stageId, user, source.isLightweight)

  constructor(stage: Stage, user: String?) :
    this(stage.execution.type, stage.execution.id, stage.execution.application, stage.id, user)
}

@JsonTypeName("resumeStage")
data class ResumeStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: ExecutionLevel, stageId: String) :
    this(source.executionType, source.executionId, source.application, stageId, source.lightweight)

  constructor(source: Stage, lightweight: Boolean = false) :
    this(source.execution.type, source.execution.id, source.execution.application, source.id, lightweight)
}

@JsonTypeName("cancelStage")
data class CancelStage(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : Message(), StageLevel {
  constructor(source: StageLevel, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, source.stageId, lightweight)

  constructor(stage: Stage, lightweight: Boolean = false) :
    this(stage.execution.type, stage.execution.id, stage.execution.application, stage.id, lightweight)
}

@JsonTypeName("startExecution")
data class StartExecution(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val lightweight: Boolean = false
) : Message(), ExecutionLevel {
  constructor(source: Execution) :
    this(source.type, source.id, source.application, source.isLightweight)
}

@JsonTypeName("rescheduleExecution")
data class RescheduleExecution(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val lightweight: Boolean = false
) : Message(), ExecutionLevel {
  constructor(source: Execution) :
    this(source.type, source.id, source.application, source.isLightweight)
}

@JsonTypeName("completeExecution")
data class CompleteExecution(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val lightweight: Boolean = false
) : Message(), ExecutionLevel {
  constructor(source: ExecutionLevel, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, lightweight)

  constructor(source: Execution, lightweight: Boolean = false) :
    this(source.type, source.id, source.application, lightweight)
}

@JsonTypeName("resumeExecution")
data class ResumeExecution(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val lightweight: Boolean = false
) : Message(), ExecutionLevel {
  constructor(source: Execution) :
    this(source.type, source.id, source.application, source.isLightweight)
}

@JsonTypeName("cancelExecution")
data class CancelExecution(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  val user: String?,
  val reason: String?,
  override val lightweight: Boolean = false
) : Message(), ExecutionLevel {
  constructor(source: Execution, user: String?, reason: String?) :
    this(source.type, source.id, source.application, user, reason, source.isLightweight)

  constructor(source: Execution) :
    this(source.type, source.id, source.application, null, null)

  constructor(source: ExecutionLevel) :
    this(source.executionType, source.executionId, source.application, null, null)
}

@JsonTypeName("startWaitingExecutions")
data class StartWaitingExecutions(
  val pipelineConfigId: String,
  val purgeQueue: Boolean = false
) : Message()

/**
 * Fatal errors in processing the execution configuration.
 */
sealed class ConfigurationError : Message(), ExecutionLevel

/**
 * Execution id was not found in the [ExecutionRepository].
 */
@JsonTypeName("invalidExecutionId")
data class InvalidExecutionId(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val lightweight: Boolean = false
) : ConfigurationError() {
  constructor(source: ExecutionLevel) :
    this(source.executionType, source.executionId, source.application)
}

/**
 * Stage id was not found in the execution.
 */
@JsonTypeName("invalidStageId")
data class InvalidStageId(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val lightweight: Boolean = false
) : ConfigurationError(), StageLevel {
  constructor(source: StageLevel) :
    this(source.executionType, source.executionId, source.application, source.stageId)
}

/**
 * Task id was not found in the stage.
 */
@JsonTypeName("invalidTaskId")
data class InvalidTaskId(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  override val lightweight: Boolean = false
) : ConfigurationError(), TaskLevel {
  constructor(source: TaskLevel) :
    this(source.executionType, source.executionId, source.application, source.stageId, source.taskId)
}

/**
 * No such [Task] class.
 */
@JsonTypeName("invalidTaskType")
data class InvalidTaskType(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  val className: String,
  override val lightweight: Boolean = false
) : ConfigurationError(), StageLevel {
  constructor(source: StageLevel, className: String) :
    this(source.executionType, source.executionId, source.application, source.stageId, className)
}

@JsonTypeName("noDownstreamTasks")
data class NoDownstreamTasks(
  override val executionType: ExecutionType,
  override val executionId: String,
  override val application: String,
  override val stageId: String,
  override val taskId: String,
  override val lightweight: Boolean = false
) : ConfigurationError(), TaskLevel {
  constructor(source: TaskLevel, lightweight: Boolean = false) :
    this(source.executionType, source.executionId, source.application, source.stageId, source.taskId, lightweight)
}

@Deprecated("Kept only to support old messages on the queue without having to do a migration")
@JsonTypeName("totalThrottleTime")
data class TotalThrottleTimeAttribute(var totalThrottleTimeMs: Long = 0) : Attribute {
  fun add(throttleTimeMs: Long) {
    this.totalThrottleTimeMs += throttleTimeMs
  }
}
