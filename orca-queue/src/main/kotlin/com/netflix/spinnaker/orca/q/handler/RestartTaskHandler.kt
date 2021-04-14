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

package com.netflix.spinnaker.orca.q.handler

import com.netflix.spinnaker.orca.ExecutionStatus.RUNNING
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.model.StageContext
import com.netflix.spinnaker.orca.pipeline.model.Task
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.orca.q.RestartTask
import com.netflix.spinnaker.orca.q.StartTask
import com.netflix.spinnaker.q.Queue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class RestartTaskHandler(
  override val queue: Queue,
  override val repository: ExecutionRepository,
  private val clock: Clock
) : OrcaMessageHandler<RestartTask> {

  override val messageType = RestartTask::class.java

  private val log: Logger get() = LoggerFactory.getLogger(javaClass)

  override fun handle(message: RestartTask) {
    message.withTaskLightweight { stage, task ->
      log.info("Begin to handle RestartTask message: $message")

      if (task.status.isComplete) {
        if (stage.execution.status.isComplete) {
          // update execution status to RUNNING
          repository.updateStatus(message.executionType, message.executionId, RUNNING)
        }
        if (stage.status.isComplete) {
          task.endTime = null
          // update stage status to RUNNING
          stage.resetLightweight()
          stage.addRestartDetails(message.user, task)
          repository.storeStage(stage)
          queue.push(StartTask(stage, task.id, message.lightweight))
        } else {
          log.error("Current stage is not complete, cannot restart task. RestartTask message: $message")
        }
      } else {
        log.error("Current task is not complete, cannot restart task. RestartTask message: $message")
      }

      log.info("End to handle RestartTask message: $message")
    }
  }

  private fun Stage.resetLightweight() {
    status = RUNNING
    // startTime has to be reset, because RunTaskHandler will checkForStageTimeout
    startTime = clock.millis()
    endTime = null
  }

  @Suppress("UNCHECKED_CAST")
  private fun Stage.addRestartDetails(user: String?, task: Task) {
    val stageContext = context as StageContext
    val restartTaskDetails = stageContext
      .getCurrentOnly("restartTaskDetails", mutableListOf<Map<String, Any?>>()) as MutableList<Map<String, Any?>>
    restartTaskDetails.add(mapOf(
      "restartedBy" to (user ?: "anonymous"),
      "restartTime" to clock.millis(),
      "restartTaskName" to task.name,
      "previousException" to stageContext.remove("exception")
    ))
    if (!stageContext.containsKey("restartTaskDetails")) {
      stageContext["restartTaskDetails"] = restartTaskDetails
    }
  }

}
