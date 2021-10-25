/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * Datum representing a localized resource. Holds the statemachine of a
 * resource. State of the resource is one of {@link ResourceState}.
 * 
 */
public class LocalizedResource implements EventHandler<ResourceEvent> {

  private static final Logger LOG =
       LoggerFactory.getLogger(LocalizedResource.class);

  volatile Path localPath;
  volatile long size = -1;
  final LocalResourceRequest rsrc; // 具体本地资源请求，LocalizedResource对象就是为此而建的
  final Dispatcher dispatcher;
  final StateMachine<ResourceState,ResourceEventType,ResourceEvent>
    stateMachine;
  final Semaphore sem = new Semaphore(1);
  final Queue<ContainerId> ref; // Queue of containers using this localized
                                // resource
  private final Lock readLock;
  private final Lock writeLock;

  final AtomicLong timestamp = new AtomicLong(currentTime());

  private static final StateMachineFactory<LocalizedResource,ResourceState,
      ResourceEventType,ResourceEvent> stateMachineFactory =
        new StateMachineFactory<LocalizedResource,ResourceState,
          ResourceEventType,ResourceEvent>(ResourceState.INIT)

    // From INIT (ref == 0, awaiting req)
    .addTransition(ResourceState.INIT, ResourceState.DOWNLOADING,
        ResourceEventType.REQUEST, new FetchResourceTransition())
    .addTransition(ResourceState.INIT, ResourceState.LOCALIZED,
        ResourceEventType.RECOVERED, new RecoveredTransition())

    // From DOWNLOADING (ref > 0, may be localizing)
    .addTransition(ResourceState.DOWNLOADING, ResourceState.DOWNLOADING,
        ResourceEventType.REQUEST, new FetchResourceTransition()) // TODO: Duplicate addition!!
    .addTransition(ResourceState.DOWNLOADING, ResourceState.LOCALIZED,
        ResourceEventType.LOCALIZED, new FetchSuccessTransition())
    .addTransition(ResourceState.DOWNLOADING,ResourceState.DOWNLOADING,
        ResourceEventType.RELEASE, new ReleaseTransition())
    .addTransition(ResourceState.DOWNLOADING, ResourceState.FAILED,
        ResourceEventType.LOCALIZATION_FAILED, new FetchFailedTransition())

    // From LOCALIZED (ref >= 0, on disk)
    .addTransition(ResourceState.LOCALIZED, ResourceState.LOCALIZED,
        ResourceEventType.REQUEST, new LocalizedResourceTransition())
    .addTransition(ResourceState.LOCALIZED, ResourceState.LOCALIZED,
        ResourceEventType.RELEASE, new ReleaseTransition())
    .installTopology(); // 添加了很多跳变规则之后调用installTopology()

  public LocalizedResource(LocalResourceRequest rsrc, Dispatcher dispatcher) {
    this.rsrc = rsrc; // 这是具体的资源请求
    this.dispatcher = dispatcher; // 准备用于这个状态机的Dispatcher
    this.ref = new LinkedList<ContainerId>();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock(); // 用来保护并发读操作的锁
    this.writeLock = readWriteLock.writeLock(); // 用来保护并发写操作的锁

    this.stateMachine = stateMachineFactory.make(this); // 生成状态机
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ").append(rsrc.toString()).append(",")
      .append(getState() == ResourceState.LOCALIZED
          ? getLocalPath() + "," + getSize()
          : "pending").append(",[");
    this.readLock.lock();
    try {
      for (ContainerId c : ref) {
        sb.append("(").append(c.toString()).append(")");
      }
      sb.append("],").append(getTimestamp()).append(",").append(getState())
        .append("}");
      return sb.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  private void release(ContainerId container) {
    if (ref.remove(container)) {
      // updating the timestamp only in case of success.
      timestamp.set(currentTime());
    } else {
      LOG.info("Container " + container
          + " doesn't exist in the container list of the Resource " + this
          + " to which it sent RELEASE event");
    }
  }

  private long currentTime() {
    return System.nanoTime();
  }

  public ResourceState getState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  public LocalResourceRequest getRequest() {
    return rsrc;
  }

  public Path getLocalPath() {
    return localPath;
  }

  public void setLocalPath(Path localPath) {
    this.localPath = Path.getPathWithoutSchemeAndAuthority(localPath);
  }

  public long getTimestamp() {
    return timestamp.get();
  }

  public long getSize() {
    return size;
  }

  public int getRefCount() {
    return ref.size();
  }

  public boolean tryAcquire() {
    return sem.tryAcquire();
  }

  public void unlock() {
    sem.release();
  }

  @Override
  public void handle(ResourceEvent event) {
    this.writeLock.lock(); // 加锁
    try {
      Path resourcePath = event.getLocalResourceRequest().getPath(); // 文件路径
      LOG.debug("Processing {} of type {}", resourcePath, event.getType());
      ResourceState oldState = this.stateMachine.getCurrentState();
      ResourceState newState = null;
      try {
        newState = this.stateMachine.doTransition(event.getType(), event); // 获取状态机的当前状态
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state", e);
      }
      if (newState != null && oldState != newState) { // 如果发生状态变化就记入日志
        LOG.debug("Resource {}{} size : {} transitioned from {} to {}",
            resourcePath, (localPath != null ? "(->" + localPath + ")": ""),
            getSize(), oldState, newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  static abstract class ResourceTransition implements
      SingleArcTransition<LocalizedResource,ResourceEvent> {
    // typedef
  }

  /**
   * Transition from INIT to DOWNLOADING.
   * Sends a {@link LocalizerResourceRequestEvent} to the
   * {@link ResourceLocalizationService}.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class FetchResourceTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceRequestEvent req = (ResourceRequestEvent) event; // 参数event实际上是个ResourceRequestEvent
      LocalizerContext ctxt = req.getContext(); // 从中抽取LocalizerContext
      ContainerId container = ctxt.getContainerId(); // 再从LocalizerContext中抽取ContainerId
      rsrc.ref.add(container); // 将ContainerId加入LocalizedResource对象rsrc
      // LocalizerResourceRequestEvent 创建（其实是重新构造）一个LocalizerResourceRequestEvent事件
      rsrc.dispatcher.getEventHandler().handle(
          new LocalizerResourceRequestEvent(rsrc, req.getVisibility(), ctxt, 
              req.getLocalResourceRequest().getPattern())); // 并处理这个事件 == ResouceLocalizationService.handle(e),用这个事件想ResouceLocalizationService提出请求
    }
  }

  /**
   * Resource localized, notify waiting containers.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class FetchSuccessTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceLocalizedEvent locEvent = (ResourceLocalizedEvent) event; // 这实际上是个ResourceLocalizedEvent对象
      rsrc.localPath =
          Path.getPathWithoutSchemeAndAuthority(locEvent.getLocation()); // 搭载在ResourceLocalizedEvent上的资源路径名是个带SchemeAndAuthority的URI，现在从中抽取文件路径名的那一部分
      rsrc.size = locEvent.getSize();
      for (ContainerId container : rsrc.ref) { // 给所涉及的每个容器发送一个ContainerEventType.RESOURCE_LOCALIZED事件
        rsrc.dispatcher.getEventHandler().handle(
            new ContainerResourceLocalizedEvent(
              container, rsrc.rsrc, rsrc.localPath)); // 用此事件驱动ContainerImpl的状态机
      }
    }
  }

  /**
   * Resource localization failed, notify waiting containers.
   */
  @SuppressWarnings("unchecked")
  private static class FetchFailedTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceFailedLocalizationEvent failedEvent =
          (ResourceFailedLocalizationEvent) event;
      Queue<ContainerId> containers = rsrc.ref;
      for (ContainerId container : containers) {
        rsrc.dispatcher.getEventHandler().handle(
          new ContainerResourceFailedEvent(container, failedEvent
            .getLocalResourceRequest(), failedEvent.getDiagnosticMessage()));
      }
    }
  }

  /**
   * Resource already localized, notify immediately.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  private static class LocalizedResourceTransition
      extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      // notify waiting containers
      ResourceRequestEvent reqEvent = (ResourceRequestEvent) event;
      ContainerId container = reqEvent.getContext().getContainerId();
      rsrc.ref.add(container);
      rsrc.dispatcher.getEventHandler().handle(
          new ContainerResourceLocalizedEvent(
            container, rsrc.rsrc, rsrc.localPath));
    }
  }

  /**
   * Decrement resource count, update timestamp.
   */
  private static class ReleaseTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      // Note: assumes that localizing container must succeed or fail
      ResourceReleaseEvent relEvent = (ResourceReleaseEvent) event;
      rsrc.release(relEvent.getContainer());
    }
  }

  private static class RecoveredTransition extends ResourceTransition {
    @Override
    public void transition(LocalizedResource rsrc, ResourceEvent event) {
      ResourceRecoveredEvent recoveredEvent = (ResourceRecoveredEvent) event;
      rsrc.localPath = recoveredEvent.getLocalPath();
      rsrc.size = recoveredEvent.getSize();
    }
  }
}
