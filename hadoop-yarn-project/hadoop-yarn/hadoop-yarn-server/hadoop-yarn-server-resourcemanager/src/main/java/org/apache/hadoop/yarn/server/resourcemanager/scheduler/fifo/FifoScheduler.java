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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FifoScheduler extends
    AbstractYarnScheduler<FifoAppAttempt, FiCaSchedulerNode> implements
    Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FifoScheduler.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;

  private boolean usePortForNodeName;

  private ActiveUsersManager activeUsersManager;

  private static final String DEFAULT_QUEUE_NAME = "default";
  private QueueMetrics metrics;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo( 
        boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(1.0f);
      Resource clusterResource = getClusterResource();
      if (clusterResource.getMemorySize() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        queueInfo.setCurrentCapacity((float) usedResource.getMemorySize()
            / clusterResource.getMemorySize());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
        new HashMap<QueueACL, AccessControlList>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
        UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo = 
        recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      return getQueueAcls().get(acl).isUserAllowed(user);
    }
    
    @Override
    public ActiveUsersManager getAbstractUsersManager() {
      return activeUsersManager;
    }

    @Override
    public void recoverContainer(Resource clusterResource,
        SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }
      increaseUsedResources(rmContainer);
      updateAppHeadRoom(schedulerAttempt);
      updateAvailableResourcesMetrics();
    }

    @Override
    public Set<String> getAccessibleNodeLabels() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public String getDefaultNodeLabelExpression() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    }

    @Override
    public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    }

    @Override
    public Priority getDefaultApplicationPriority() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }

    @Override
    public void decReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }
  };

  public FifoScheduler() {
    super(FifoScheduler.class.getName());
  }

  private synchronized void initScheduler(Configuration conf) {
    validateConf(conf);
    //Use ConcurrentSkipListMap because applications need to be ordered
    this.applications =
        new ConcurrentSkipListMap<>();
    this.minimumAllocation = super.getMinimumAllocation();
    initMaximumResourceCapability(super.getMaximumAllocation());
    this.usePortForNodeName = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false,
        conf);
    this.activeUsersManager = new ActiveUsersManager(metrics);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);

    // Initialize SchedulingMonitorManager
    schedulingMonitorManager.initialize(rmContext, conf);
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }
  
  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void
      reinitialize(Configuration conf, RMContext rmContext) throws IOException
  {
    setConf(conf);
    super.reinitialize(conf, rmContext);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    FifoAppAttempt application = getApplicationAttempt(applicationAttemptId); // 代表着要求分配资源的AppAttempt
    if (application == null) {
      LOG.error("Calling allocate on removed or non existent application " +
          applicationAttemptId.getApplicationId());
      return EMPTY_ALLOCATION;
    }

    // The allocate may be the leftover from previous attempt, and it will
    // impact current attempt, such as confuse the request and allocation for
    // current attempt's AM container.
    // Note outside precondition check for the attempt id may be
    // outdated here, so double check it here is necessary.
    if (!application.getApplicationAttemptId().equals(applicationAttemptId)) {
      LOG.error("Calling allocate on previous or removed " +
          "or non existent application attempt " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check 资源要求的合理性检测和规格化
    normalizeResourceRequests(ask);

    // Release containers 释放该释放的容器
    releaseContainers(release, application);

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
            "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) { // 要求分配的资源集合非空
        LOG.debug("allocate: pre-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        LOG.debug("allocate:" +
            " applicationId=" + applicationAttemptId +
            " #ask=" + ask.size());
      }
      // 修改黑名单
      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      Resource headroom = application.getHeadroom();
      application.setApplicationHeadroomForMetrics(headroom);
      // application.pullNewlyAllocatedContainers() 把新分配的容器都收揽过啦并为之创建Token

      return new Allocation(application.pullNewlyAllocatedContainers(),
          headroom, null, null, null, application.pullUpdatedNMTokens());
    }
  }

  @VisibleForTesting
  public synchronized void addApplication(ApplicationId applicationId,
      String queue, String user, boolean isAppRecovering) {
    SchedulerApplication<FifoAppAttempt> application =
        new SchedulerApplication<>(DEFAULT_QUEUE, user);
    applications.put(applicationId, application); // 将创建的SchedulerApplication放在applications集合里
    metrics.submitApp(user); // metrics是一个QueueMetrics对象，用于统计目的
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", currently num of applications: " + applications.size());
    if (isAppRecovering) {
      LOG.debug("{} is recovering. Skip notifying APP_ACCEPTED",
          applicationId);
    } else {
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED)); // 创建APP_ACCEPTED事件，并派发事件
    }
  }

  @VisibleForTesting
  public synchronized void
      addApplicationAttempt(ApplicationAttemptId appAttemptId,
          boolean transferStateFromPreviousAttempt,
          boolean isAttemptRecovering) {
    SchedulerApplication<FifoAppAttempt> application =
        applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    // TODO: Fix store
    FifoAppAttempt schedulerApp =
        new FifoAppAttempt(appAttemptId, user, DEFAULT_QUEUE,
          activeUsersManager, this.rmContext);

    if (transferStateFromPreviousAttempt) {
      schedulerApp.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(schedulerApp);

    metrics.submitAppAttempt(user);
    LOG.info("Added Application Attempt " + appAttemptId
        + " to scheduler from user " + application.getUser());
    if (isAttemptRecovering) {
      LOG.debug("{} is recovering. Skipping notifying ATTEMPT_ADDED",
          appAttemptId);
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  private synchronized void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication<FifoAppAttempt> application =
        applications.get(applicationId);
    if (application == null){
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }

    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(application.getUser(),
      applicationId);
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void doneApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers)
      throws IOException {
    FifoAppAttempt attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication<FifoAppAttempt> application =
        applications.get(applicationAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException("Unknown application " + applicationAttemptId + 
      " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers
          && container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + container.getContainerId());
        continue;
      }
      super.completedContainer(container,
        SchedulerUtils.createAbnormalContainerStatus(
          container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
        RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);
  }
  
  /**
   * Heart of the scheduler...
   * 
   * @param node node on which resources are available to be allocated
   */
  private void assignContainers(FiCaSchedulerNode node) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " #applications=" + applications.size());

    // Try to assign containers to applications in fifo order
    // 尝试按 fifo 顺序将容器分配给应用程序
    for (Map.Entry<ApplicationId, SchedulerApplication<FifoAppAttempt>> e : applications
        .entrySet()) {
      // 获取该App的当前AppAttempt对象
      FifoAppAttempt application = e.getValue().getCurrentAppAttempt();
      if (application == null) {
        continue;
      }

      LOG.debug("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        // 如果这个App不适合放在这个节点上，已将本节点列入其黑名单，就跳过
        if (SchedulerAppUtils.isPlaceBlacklisted(application, node, LOG)) {
          continue;
        }

        for (SchedulerRequestKey schedulerKey :
            application.getSchedulerKeys()) {
          // 获取最大可分配容器
          int maxContainers =
              getMaxAllocatableContainers(application, schedulerKey, node,
                  NodeType.OFF_SWITCH);
          // Ensure the application needs containers of this priority
          // 确保应用程序需要此优先级的容器,对于同一App不同优先级别的容器要求
          if (maxContainers > 0) {
            int assignedContainers =
                assignContainersOnNode(node, application, schedulerKey);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        } // 已完成对同一APP所有优先级别资源要求的扫描
      }
      
      LOG.debug("post-assignContainers");
      application.showRequests();

      // Done 资源不足就循环结束
      if (Resources.lessThan(resourceCalculator, getClusterResource(),
              node.getUnallocatedResource(), minimumAllocation)) {
        break;
      }
    } // 已完成对所有已受理App的扫描

    // Update the applications' headroom to correctly take into
    // account the containers assigned in this update.
    // 更新应用程序的净空以正确考虑
    // 在此更新中分配的容器。
    for (SchedulerApplication<FifoAppAttempt> application : applications.values()) {
      FifoAppAttempt attempt =
          (FifoAppAttempt) application.getCurrentAppAttempt();
      if (attempt == null) {
        continue;
      }
      updateAppHeadRoom(attempt); // 修过各个FiCaSchedulerApp的可用资源上限
    }
  }

  /**
   * 获取最多能为本App分配多少个容器
   * @param application
   * @param schedulerKey
   * @param node
   * @param type
   * @return
   */
  private int  getMaxAllocatableContainers(FifoAppAttempt application,
      SchedulerRequestKey schedulerKey, FiCaSchedulerNode node, NodeType type) {
    PendingAsk offswitchAsk = application.getPendingAsk(schedulerKey,
        ResourceRequest.ANY); // 获取该App总的资源需求，不考虑地狱
    int maxContainers = offswitchAsk.getCount(); // 该App所需的容器数量

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      PendingAsk rackLocalAsk = application.getPendingAsk(schedulerKey,
          node.getRackName()); // 获取该app可用分配在此机架上的资源需求
      if (rackLocalAsk.getCount() <= 0) { //
        return maxContainers;
      }
      // 该App可用分配在此机架上的容器数量
      maxContainers = Math.min(maxContainers,
          rackLocalAsk.getCount());
    }

    if (type == NodeType.NODE_LOCAL) {
      PendingAsk nodeLocalAsk = application.getPendingAsk(schedulerKey,
          node.getRMNode().getHostName()); // 获取该App在此节点上的资源需求

      if (nodeLocalAsk.getCount() > 0) {
        maxContainers = Math.min(maxContainers,
            nodeLocalAsk.getCount()); // 该APP所需在此节点上的容器数量
      }
    }

    return maxContainers;
  }


  private int assignContainersOnNode(FiCaSchedulerNode node, 
      FifoAppAttempt application, SchedulerRequestKey schedulerKey
  ) {
    // Data-local 先尝试满足其指定需要在这个节点上的容器要求
    int nodeLocalContainers =
        assignNodeLocalContainers(node, application, schedulerKey);

    // Rack-local 再尝试满足可以在同一机架上的容器要求
    int rackLocalContainers =
        assignRackLocalContainers(node, application, schedulerKey);

    // Off-switch 最后试图满足其余的容器要求
    int offSwitchContainers =
        assignOffSwitchContainers(node, application, schedulerKey);


    LOG.debug("assignContainersOnNode:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " application=" + application.getApplicationId().getId() +
        " priority=" + schedulerKey.getPriority() +
        " #assigned=" + 
        (nodeLocalContainers + rackLocalContainers + offSwitchContainers));


    return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeLocalContainers(FiCaSchedulerNode node, 
      FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk nodeLocalAsk = application.getPendingAsk(schedulerKey,
        node.getNodeName());
    if (nodeLocalAsk.getCount() > 0) {
      // Don't allocate on this node if we don't need containers on this rack
      // 如果我们不需要这个机架上的容器，就不要在这个节点上分配
      // APP没有要求这个节点上的资源，就返回
      if (application.getOutstandingAsksCount(schedulerKey,
          node.getRackName()) <= 0) {
        return 0;
      }

      int assignableContainers = Math.min(
          getMaxAllocatableContainers(application, schedulerKey, node,
              NodeType.NODE_LOCAL), nodeLocalAsk.getCount());
      assignedContainers = 
        assignContainer(node, application, schedulerKey, assignableContainers,
            nodeLocalAsk.getPerAllocationResource(), NodeType.NODE_LOCAL);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(FiCaSchedulerNode node, 
      FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk rackAsk = application.getPendingAsk(schedulerKey,
        node.getRMNode().getRackName());
    if (rackAsk.getCount() > 0) {
      // Don't allocate on this rack if the application doens't need containers
      // 如果App仅在这个一个机架上有容器要求，就先不分配
      if (application.getOutstandingAsksCount(schedulerKey,
          ResourceRequest.ANY) <= 0) {
        return 0;
      }

      int assignableContainers =
          Math.min(getMaxAllocatableContainers(application, schedulerKey, node,
              NodeType.RACK_LOCAL), rackAsk.getCount());
      assignedContainers = 
        assignContainer(node, application, schedulerKey, assignableContainers,
            rackAsk.getPerAllocationResource(), NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(FiCaSchedulerNode node, 
      FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk offswitchAsk = application.getPendingAsk(schedulerKey,
        ResourceRequest.ANY);
    if (offswitchAsk.getCount() > 0) {
      assignedContainers = 
        assignContainer(node, application, schedulerKey,
            offswitchAsk.getCount(),
            offswitchAsk.getPerAllocationResource(), NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainer(FiCaSchedulerNode node, FifoAppAttempt application,
      SchedulerRequestKey schedulerKey, int assignableContainers,
      Resource capability, NodeType type) {
    LOG.debug("assignContainers:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " application=" + application.getApplicationId().getId() + 
        " priority=" + schedulerKey.getPriority().getPriority() +
        " assignableContainers=" + assignableContainers +
        " capability=" + capability + " type=" + type);

    // TODO: A buggy application with this zero would crash the scheduler.
    // 从内存资源角度计算该节点上容纳几份的任务
    int availableContainers =
        (int) (node.getUnallocatedResource().getMemorySize() /
                capability.getMemorySize());
    // 需求和可能，取其小者
    int assignedContainers =
      Math.min(assignableContainers, availableContainers);

    if (assignedContainers > 0) {
      for (int i=0; i < assignedContainers; ++i) { // 逐一分配这些容器

        NodeId nodeId = node.getRMNode().getNodeID();
        // 生成一个ID
        ContainerId containerId = BuilderUtils.newContainerId(application
            .getApplicationAttemptId(), application.getNewContainerId());

        // Create the container，创建容器
        Container container = BuilderUtils.newContainer(containerId, nodeId,
            node.getRMNode().getHttpAddress(), capability,
            schedulerKey.getPriority(), null,
            schedulerKey.getAllocationRequestId());
        
        // Allocate!
        
        // Inform the application
        // FiCaSchedulerApp.allocate，为该容器创建RMContainerImpl对象
        RMContainer rmContainer = application.allocate(type, node, schedulerKey,
            container);

        // Inform the node
        // 调整该节点上的容器和资源数量，并将此容器放入该节点的launchedContainers集合
        node.allocateContainer(rmContainer);

        // Update usage for this container
        // 更新此容器的使用情况
        increaseUsedResources(rmContainer);
      } // 完成一个容器的分配，继续循环

    }
    
    return assignedContainers;  // 返回分配的容器数量
  }

  private void increaseUsedResources(RMContainer rmContainer) {
    Resources.addTo(usedResource, rmContainer.getAllocatedResource());
  }

  private void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
    schedulerAttempt.setHeadroom(Resources.subtract(getClusterResource(),
      usedResource));
  }

  private void updateAvailableResourcesMetrics() {
    metrics.setAvailableResourcesToQueue(
        Resources.subtract(getClusterResource(), usedResource));
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED: // 增加了一个节点
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
        nodeAddedEvent.getAddedRMNode());

    }
    break;
    case NODE_REMOVED: // 移除了一个节点
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_RESOURCE_UPDATE: // 节点的资源发生了变化
    {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = 
          (NodeResourceUpdateSchedulerEvent)event;
      updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
        nodeResourceUpdatedEvent.getResourceOption());
    }
    break;
    case NODE_UPDATE: // 节点的状态发生了变化
    {
      NodeUpdateSchedulerEvent nodeUpdatedEvent = 
      (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
    }
    break;
    case APP_ADDED: // 增加了一个App
    {
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser(),
        appAddedEvent.getIsAppRecovering());
    }
    break;
    case APP_REMOVED: // 移除了一个App
    {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      doneApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED: // 试图增加一个app
    {
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
        appAttemptAddedEvent.getIsAttemptRecovering());
    }
    break;
    case APP_ATTEMPT_REMOVED: // 试图移除一个app
    {
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      try {
        doneApplicationAttempt(
          appAttemptRemovedEvent.getApplicationAttemptID(),
          appAttemptRemovedEvent.getFinalAttemptState(),
          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
      } catch(IOException ie) {
        LOG.error("Unable to remove application "
            + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
      }
    }
    break;
    case CONTAINER_EXPIRED: // 容器过期
    {
      ContainerExpiredSchedulerEvent containerExpiredEvent = 
          (ContainerExpiredSchedulerEvent) event;
      ContainerId containerid = containerExpiredEvent.getContainerId();
      super.completedContainer(getRMContainer(containerid),
          SchedulerUtils.createAbnormalContainerStatus(
              containerid, 
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
    }
    break;
    case RELEASE_CONTAINER: { // 释放容器
      if (!(event instanceof ReleaseContainerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      RMContainer container = ((ReleaseContainerEvent) event).getContainer();
      completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(),
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  @Lock(FifoScheduler.class)
  @Override
  protected synchronized void completedContainerInternal(
      RMContainer rmContainer, ContainerStatus containerStatus,
      RMContainerEventType event) {

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    FifoAppAttempt application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    
    // Get the node on which the container was allocated
    FiCaSchedulerNode node = (FiCaSchedulerNode) getNode(container.getNodeId());
    
    if (application == null) {
      LOG.info("Unknown application: " + appId + 
          " released container " + container.getId() +
          " on node: " + node + 
          " with event: " + event);
      return;
    }

    // Inform the application 通知容器
    application.containerCompleted(rmContainer, containerStatus, event,
        RMNodeLabelsManager.NO_LABEL);

    // Inform the node
    node.releaseContainer(rmContainer.getContainerId(), false);
    
    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());

    LOG.info("Application attempt " + application.getApplicationAttemptId() + 
        " released container " + container.getId() +
        " on node: " + node + 
        " with event: " + event);
     
  }
  
  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private synchronized void removeNode(RMNode nodeInfo) {
    FiCaSchedulerNode node = nodeTracker.getNode(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    // Kill running containers
    for(RMContainer container : node.getCopiedListOfRunningContainers()) {
      super.completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(), 
              SchedulerUtils.LOST_CONTAINER),
              RMContainerEventType.KILL);
    }
    nodeTracker.removeNode(nodeInfo.getNodeID());
  }

  @Override
  public QueueInfo getQueueInfo(String queueName,
      boolean includeChildQueues, boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null); 
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  private synchronized void addNode(RMNode nodeManager) {
    FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
        usePortForNodeName);
    nodeTracker.addNode(schedulerNode);
  }

  @Override
  public void recover(RMState state) {
    // NOT IMPLEMENTED
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FifoAppAttempt attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
  }

  @Override
  public synchronized List<ApplicationAttemptId>
      getAppsInQueue(String queueName) {
    if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
      List<ApplicationAttemptId> attempts =
          new ArrayList<ApplicationAttemptId>(applications.size());
      for (SchedulerApplication<FifoAppAttempt> app : applications.values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }

  public Resource getUsedResource() {
    return usedResource;
  }

  @Override
  protected synchronized void nodeUpdate(RMNode nm) {
    super.nodeUpdate(nm);

    FiCaSchedulerNode node = (FiCaSchedulerNode) getNode(nm.getNodeID());
    if (rmContext.isWorkPreservingRecoveryEnabled()
        && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }

    // A decommissioned node might be removed before we get here
    if (node != null &&
        Resources.greaterThanOrEqual(resourceCalculator, getClusterResource(),
            node.getUnallocatedResource(), minimumAllocation)) {
      // 若可供分配的资源达到了门槛值minimumAllocation，就分配容器
      LOG.debug("Node heartbeat " + nm.getNodeID() +
          " available resource = " + node.getUnallocatedResource());
      // 分配容器，重点
      assignContainers(node);

      LOG.debug("Node after allocation " + nm.getNodeID() + " resource = "
          + node.getUnallocatedResource());
    }

    updateAvailableResourcesMetrics();
  }

  @VisibleForTesting
  @Override
  public void killContainer(RMContainer container) {
    ContainerStatus status = SchedulerUtils.createKilledContainerStatus(
        container.getContainerId(),
        "Killed by RM to simulate an AM container failure");
    LOG.info("Killing container " + container);
    completedContainer(container, status, RMContainerEventType.KILL);
  }

  @Override
  public synchronized void recoverContainersOnNode(
      List<NMContainerStatus> containerReports, RMNode nm) {
    super.recoverContainersOnNode(containerReports, nm);
  }
}
