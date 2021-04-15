/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.listeners

import groovy.util.logging.Slf4j
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent
import org.springframework.context.ApplicationEvent
import org.springframework.context.event.SmartApplicationListener
import org.springframework.stereotype.Component

/**
 * 为了解决logback多环境配置文件加载问题
 *
 * <ul>
 *   <li>springboot默认log加载机制：LoggingApplicationListener--onApplicationEnvironmentPreparedEvent--LogbackLoggingSystem
 *   <li>logback默认配置（logback-test.groovy,logback-test.xml,logback.groovy,logback.xml）的加载早于application.yml
 * </ul>
 *
 * Created by Yuan Chaochao on 2018/11/15 15:54
 */
@Component
@Slf4j
class LogBackConfigListener implements SmartApplicationListener {

  @Override
  void onApplicationEvent(ApplicationEvent event) {
    def env = ((ApplicationEnvironmentPreparedEvent) event).getEnvironment()
    def logConfigFile =
      String.format("classpath:logback/logback-%s.xml", env.getProperty("app.env"))
    log.info("logger file :{}", logConfigFile)
    System.setProperty("logging.config", logConfigFile)
  }

  @Override
  boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
    return eventType.equals(ApplicationEnvironmentPreparedEvent.class)
  }

  @Override
  int getOrder() {
    return HIGHEST_PRECEDENCE
  }
}
