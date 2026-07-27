/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/looplab/fsm"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"

	"github.com/apache/yunikorn-k8shim/pkg/common"
	"github.com/apache/yunikorn-k8shim/pkg/common/constants"
	"github.com/apache/yunikorn-k8shim/pkg/common/events"
	"github.com/apache/yunikorn-k8shim/pkg/common/utils"
	"github.com/apache/yunikorn-k8shim/pkg/dispatcher"
	"github.com/apache/yunikorn-k8shim/pkg/locking"
	"github.com/apache/yunikorn-k8shim/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Task struct {
	taskID        string
	alias         string
	applicationID string
	application   *Application
	podStatus     v1.PodStatus // pod status, maintained separately for efficiency reasons
	context       *Context
	createTime    time.Time
	placeholder   bool
	originator    bool
	sm            *fsm.FSM

	// mutable resources, require locking
	allocationKey   string
	nodeName        string
	taskGroupName   string
	terminationType string
	schedulingState TaskSchedulingState
	resource        *si.Resource
	pod             *v1.Pod

	lock *locking.RWMutex
}

func NewTask(tid string, app *Application, ctx *Context, pod *v1.Pod) *Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, false, "", ctx, false)
}

func NewTaskPlaceholder(tid string, app *Application, ctx *Context, pod *v1.Pod) *Task {
	taskResource := common.GetPodResource(pod)
	return createTaskInternal(tid, app, taskResource, pod, true, "", ctx, false)
}

func NewFromTaskMeta(tid string, app *Application, ctx *Context, metadata TaskMetadata, originator bool) *Task {
	taskPod := metadata.Pod
	taskResource := common.GetPodResource(taskPod)
	return createTaskInternal(
		tid,
		app,
		taskResource,
		metadata.Pod,
		metadata.Placeholder,
		metadata.TaskGroupName,
		ctx,
		originator)
}

func createTaskInternal(tid string, app *Application, resource *si.Resource,
	pod *v1.Pod, placeholder bool, taskGroupName string, ctx *Context, originator bool) *Task {
	task := &Task{
		taskID:          tid,
		alias:           fmt.Sprintf("%s/%s", pod.Namespace, pod.Name),
		applicationID:   app.GetApplicationID(),
		application:     app,
		pod:             pod,
		podStatus:       *pod.Status.DeepCopy(),
		resource:        resource,
		createTime:      pod.GetCreationTimestamp().Time,
		placeholder:     placeholder,
		taskGroupName:   taskGroupName,
		originator:      originator,
		context:         ctx,
		sm:              newTaskState(),
		schedulingState: TaskSchedPending,
		lock:            &locking.RWMutex{},
	}
	if tgName := utils.GetTaskGroupFromPodSpec(pod); tgName != "" {
		task.taskGroupName = tgName
	}
	task.initialize()
	return task
}

// event handling
func (task *Task) handle(te events.TaskEvent) error {
	task.lock.Lock()
	defer task.lock.Unlock()
	err := task.sm.Event(context.Background(), te.GetEvent(), task, te.GetArgs())
	// handle the same state transition not nil error (limit of fsm).
	if err != nil && err.Error() != "no transition" {
		return err
	}
	return nil
}

func (task *Task) canHandle(te events.TaskEvent) bool {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.sm.Can(te.GetEvent())
}

func (task *Task) GetTaskPod() *v1.Pod {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.pod
}

func (task *Task) GetTaskID() string {
	return task.taskID
}

func (task *Task) IsPlaceholder() bool {
	return task.placeholder
}

func (task *Task) GetTaskState() string {
	// fsm has its own internal lock, we don't need to hold node's lock here
	return task.sm.Current()
}

func (task *Task) setTaskGroupName(groupName string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.taskGroupName = groupName
}

func (task *Task) setTaskTerminationType(terminationType string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.terminationType = terminationType
}

func (task *Task) GetTaskTerminationType() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.terminationType
}

func (task *Task) GetTaskGroupName() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.taskGroupName
}

func (task *Task) GetNodeName() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.nodeName
}

func (task *Task) DeleteTaskPod() error {
	return task.context.apiProvider.GetAPIs().KubeClient.Delete(task.GetTaskPod())
}

func (task *Task) UpdateTaskPodStatus(pod *v1.Pod) (*v1.Pod, error) {
	return task.context.apiProvider.GetAPIs().KubeClient.UpdateStatus(pod)
}

func (task *Task) UpdateTaskPod(pod *v1.Pod, podMutator func(pod *v1.Pod)) (*v1.Pod, error) {
	return task.context.apiProvider.GetAPIs().KubeClient.UpdatePod(pod, podMutator)
}

func (task *Task) isTerminated() bool {
	for _, states := range TaskStates().Terminated {
		if task.GetTaskState() == states {
			return true
		}
	}
	return false
}

// task object initialization
// normally when task is added, the task state is New
// but during scheduler init after restart, we need to init the task state according to
// the task pod status. if the pod is already terminated,
// we should mark the task as completed according.
func (task *Task) initialize() {
	task.lock.Lock()
	defer task.lock.Unlock()

	// task already terminated, succeed or failed
	// that means the task was already allocated and completed
	// the resources were already released, instead of starting
	// from New, directly set the task to Completed
	if utils.IsPodTerminated(task.pod) {
		task.allocationKey = string(task.pod.UID)
		task.nodeName = task.pod.Spec.NodeName
		task.sm.SetState(TaskStates().Completed)
		log.Log(log.ShimCacheTask).Info("set task as Completed",
			zap.String("appID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("allocationKey", task.allocationKey),
			zap.String("nodeName", task.nodeName))
	}
}

func (task *Task) IsOriginator() bool {
	return task.originator
}

func (task *Task) isPreemptSelfAllowed() bool {
	value := utils.GetPodAnnotationValue(task.pod, constants.AnnotationAllowPreemption)
	switch value {
	case constants.True:
		return true
	case constants.False:
		return false
	default:
		return task.context.IsPreemptSelfAllowed(task.pod.Spec.PriorityClassName)
	}
}

func (task *Task) isPreemptOtherAllowed() bool {
	policy := task.pod.Spec.PreemptionPolicy
	if policy == nil {
		return true
	}
	switch *policy {
	case v1.PreemptNever:
		return false
	case v1.PreemptLowerPriority:
		return true
	default:
		return true
	}
}

func (task *Task) SetTaskSchedulingState(state TaskSchedulingState) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.schedulingState = state
}

func (task *Task) MarkPreviouslyAllocated(allocationKey string, nodeID string) {
	task.sm.SetState(TaskStates().Bound)
	task.lock.Lock()
	defer task.lock.Unlock()
	task.schedulingState = TaskSchedAllocated
	task.allocationKey = allocationKey
	task.nodeName = nodeID
	if task.placeholder {
		log.Log(log.ShimCacheTask).Info("placeholder is bound",
			zap.String("appID", task.applicationID),
			zap.String("taskName", task.alias),
			zap.String("taskGroupName", task.taskGroupName))
		dispatcher.Dispatch(NewUpdateApplicationReservationEvent(task.applicationID))
	}
}

func (task *Task) GetTaskSchedulingState() TaskSchedulingState {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.schedulingState
}

func (task *Task) handleSubmitTaskEvent() {
	log.Log(log.ShimCacheTask).Debug("scheduling pod",
		zap.String("podName", task.pod.Name))

	// send update allocation event to core
	task.updateAllocation()

	if !utils.PodAlreadyBound(task.pod) {
		// if this is a new request, add events to pod
		events.GetRecorder().Eventf(task.pod.DeepCopy(), nil, v1.EventTypeNormal, "Scheduling", "Scheduling",
			"%s is queued and waiting for allocation", task.alias)
		// if this task belongs to a task group, that means the app has gang scheduling enabled
		// in this case, post an event to indicate the task is being gang scheduled
		if !task.placeholder && task.taskGroupName != "" {
			events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
				v1.EventTypeNormal, "GangScheduling", "TaskGroupMatch",
				"Pod belongs to the taskGroup %s, it will be scheduled as a gang member", task.taskGroupName)
		}
	}
}

// updateAllocation updates the core scheduler when task information changes.
// This function must be called with the task lock held.
func (task *Task) updateAllocation() {
	// build preemption policy
	preemptionPolicy := &si.PreemptionPolicy{
		AllowPreemptSelf:  task.isPreemptSelfAllowed(),
		AllowPreemptOther: task.isPreemptOtherAllowed(),
	}

	// submit allocation
	rr := common.CreateAllocationForTask(
		task.applicationID,
		task.taskID,
		task.pod.Spec.NodeName,
		task.resource,
		task.placeholder,
		task.taskGroupName,
		task.pod,
		task.originator,
		preemptionPolicy)
	log.Log(log.ShimCacheTask).Debug("send update request", zap.Stringer("request", rr))
	if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(rr); err != nil {
		log.Log(log.ShimCacheTask).Debug("failed to send allocation to scheduler", zap.Error(err))
		return
	}
}

// this is called after task reaches PENDING state,
// submit the resource asks from this task to the scheduler core
func (task *Task) postTaskPending() {
	dispatcher.Dispatch(NewSubmitTaskEvent(task.applicationID, task.taskID))
}

// postTaskAllocated is called after task reaches ALLOCATED state.
// This routine binds the pod to the allocated node.
// It calls K8s api to bind a pod to the assigned node, this may need some time,
// so we do a delay binding, background process, to avoid blocking main process.
// The result of the binding is tracked and failures are properly handled.
// If successful, we move task to next state BOUND, otherwise we fail the task
func (task *Task) postTaskAllocated() {
	go func() {
		// we need to obtain task's lock first,
		// this ensures no other threads modifying task state at the time being
		task.lock.Lock()
		defer task.lock.Unlock()

		// post a message to indicate the pod gets its allocation
		events.GetRecorder().Eventf(task.pod.DeepCopy(),
			nil, v1.EventTypeNormal, "Scheduled", "Scheduled",
			"Successfully assigned %s to node %s", task.alias, task.nodeName)

		// before binding pod to node, first bind volumes to pod
		log.Log(log.ShimCacheTask).Debug("bind pod volumes",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))
		if err := task.context.bindPodVolumes(task.pod); err != nil {
			log.Log(log.ShimCacheTask).Error("bind volumes to pod failed", zap.String("taskID", task.taskID), zap.Error(err))
			task.failWithEvent(fmt.Sprintf("bind volumes to pod failed, name: %s, %s", task.alias, err.Error()), "PodVolumesBindFailure")
			return
		}
		log.Log(log.ShimCacheTask).Debug("bind pod",
			zap.String("podName", task.pod.Name),
			zap.String("podUID", string(task.pod.UID)))

		if err := task.context.apiProvider.GetAPIs().KubeClient.Bind(task.pod, task.nodeName); err != nil {
			log.Log(log.ShimCacheTask).Error("bind pod to node failed", zap.String("taskID", task.taskID), zap.Error(err))
			task.failWithEvent(fmt.Sprintf("bind pod to node failed, name: %s, %s", task.alias, err.Error()), "PodBindFailure")
			return
		}
		log.Log(log.ShimCacheTask).Info("successfully bound pod", zap.String("podName", task.pod.Name))
		dispatcher.Dispatch(NewBindTaskEvent(task.applicationID, task.taskID))
		events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
			v1.EventTypeNormal, "PodBindSuccessful", "PodBindSuccessful",
			"Pod %s is successfully bound to node %s", task.alias, task.nodeName)

		task.schedulingState = TaskSchedAllocated
	}()
}

// beforeTaskAllocated is called before handling the TaskAllocated event.
// This sets the allocation information returned by the core in the task. It must
// stay a before-hook: the enter-Allocated callback (postTaskAllocated) binds the
// pod using these fields, so they must be set before the transition completes.
// It only writes plain task fields and acquires no locks, so it is safe to run
// while the FSM holds its internal locks across before-callbacks.
func (task *Task) beforeTaskAllocated(allocationKey string, nodeID string) {
	// task is allocated on a node with a allocationKey set the details in the task here to allow referencing later.
	task.allocationKey = allocationKey
	task.nodeName = nodeID
}

// afterTaskAllocated runs after the TaskAllocated event, once the FSM has
// released its internal locks (see releaseAllocation for why the release must
// not run in a before-callback).
// In some cases, the task is canceled (e.g. pod deleted) before we process the
// allocation from the core. Those tasks will already be in the Completed state.
// If we find the task was already in Completed state while handling the
// TaskAllocated event, we need to explicitly release this allocation because it
// is no longer valid. eventSrc is the FSM event's source state, i.e. the task
// state at the time the TaskAllocated event was evaluated.
func (task *Task) afterTaskAllocated(eventSrc string) {
	// If the task is Completed the pod was deleted on K8s but the core was not aware yet.
	// Notify the core to release this allocation to avoid resource leak.
	// The ask is not relevant at this point.
	if eventSrc == TaskStates().Completed {
		log.Log(log.ShimCacheTask).Info("task is already completed, invalidate the allocation",
			zap.String("currentTaskState", eventSrc),
			zap.String("allocationKey", task.allocationKey),
			zap.String("allocatedNode", task.nodeName))
		task.releaseAllocation(eventSrc, false)
	}
}

func (task *Task) postTaskBound() {
	if task.placeholder {
		log.Log(log.ShimCacheTask).Info("placeholder is bound",
			zap.String("appID", task.applicationID),
			zap.String("taskName", task.alias),
			zap.String("taskGroupName", task.taskGroupName))
		dispatcher.Dispatch(NewUpdateApplicationReservationEvent(task.applicationID))
	}
}

func (task *Task) postTaskRejected() {
	// currently, once task is rejected by scheduler, we directly move task to failed state.
	// so this function simply triggers the state transition when it is rejected.
	// but further, we can introduce retry mechanism if necessary.
	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeWarning, "TaskRejected", "TaskRejected",
		"Task %s is rejected by the scheduler", task.alias)
	dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID,
		fmt.Sprintf("task %s failed because it is rejected by scheduler", task.alias)))
}

// afterTaskFail releases the allocation or ask from scheduler core.
// This was historically a before-hook so that releaseAllocation could read the
// pre-transition state through GetTaskState() (YUNIKORN-45); it is now an
// after-hook for lock safety (see releaseAllocation) and the pre-transition
// state is passed in explicitly as the FSM event's source state, which is
// exactly what GetTaskState() returned while the before-hook ran.
func (task *Task) afterTaskFail(eventSrc string) {
	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskFailed", "TaskFailed",
		"Task %s is failed", task.alias)
	task.releaseAllocation(eventSrc, false)
}

// afterTaskCompleted releases the allocation or ask from scheduler core.
// This is an after-hook for the same reasons as afterTaskFail: the release must
// run outside the FSM's internal locks (see releaseAllocation), and the
// pre-transition state it needs is passed in as the FSM event's source state.
func (task *Task) afterTaskCompleted(eventSrc string) {
	task.releaseAllocation(eventSrc, false)

	events.GetRecorder().Eventf(task.pod.DeepCopy(), nil,
		v1.EventTypeNormal, "TaskCompleted", "TaskCompleted",
		"Task %s is completed", task.alias)
}

// releaseAllocation sends the release request for the Allocation to the core.
//
// currentState is the task state the release is evaluated against. Callers in
// task FSM after-callbacks pass the event's source state (identical to what
// GetTaskState() returned before the transition, preserving the semantics of
// the historical before-hook, see YUNIKORN-45); callers outside FSM callbacks
// (e.g. flushReleaseableTasks) pass task.GetTaskState().
//
// Lock safety: this runs with the task lock held (task.handle) and must be
// lock-neutral beyond that:
//   - It must NOT run inside a task FSM before-callback: the FSM holds its
//     internal state lock (read-locked) across before-callbacks, so re-reading
//     the state here via GetTaskState() is a recursive read-lock that deadlocks
//     behind a queued FSM writer (e.g. MarkPreviouslyAllocated -> SetState),
//     and any other lock acquired here would nest inside the FSM lock window.
//     It is therefore only invoked from after-callbacks, where the FSM has
//     already released its internal locks, and it reads the state exclusively
//     through the currentState argument.
//   - It must NOT drop and re-take the task lock: another goroutine (e.g. the
//     scheduling loop calling task.handle) can grab the freed task lock and
//     block inside the FSM, after which re-acquiring the task lock deadlocks.
//   - It must NOT acquire the application lock: application FSM callbacks run
//     while the application lock is held and acquire task locks, the opposite
//     order. Deferral bookkeeping uses the dedicated leaf lock
//     releaseableTasksLock instead (see Application.tryAddReleasableTask).
func (task *Task) releaseAllocation(currentState string, force bool) {
	terminationType := common.GetTerminationTypeFromString(task.terminationType)

	if !force && task.shouldAppRelease() {
		log.Log(log.ShimCacheTask).Info("not releasing task right now, app has not been accepted",
			zap.String("appState", task.application.sm.Current()))
		return
	}

	// scheduler api might be nil in some tests
	if task.context.apiProvider.GetAPIs().SchedulerAPI != nil {
		log.Log(log.ShimCacheTask).Debug("prepare to send release request",
			zap.String("applicationID", task.applicationID),
			zap.String("taskID", task.taskID),
			zap.String("taskAlias", task.alias),
			zap.String("allocationKey", task.allocationKey),
			zap.String("task", currentState),
			zap.String("terminationType", string(terminationType)))

		// send an AllocationReleaseRequest
		var releaseRequest *si.AllocationRequest
		s := TaskStates()

		// check if the task is in a state where it has not been allocated yet
		if currentState != s.New && currentState != s.Pending &&
			currentState != s.Scheduling && currentState != s.Rejected {
			// task is in a state where it might have been allocated
			if task.allocationKey == "" {
				log.Log(log.ShimCacheTask).Warn("BUG: task allocationKey is empty on release",
					zap.String("applicationID", task.applicationID),
					zap.String("taskID", task.taskID),
					zap.String("taskAlias", task.alias),
					zap.String("taskState", currentState))
			}
		}

		// create the release request
		releaseRequest = common.CreateReleaseRequestForTask(
			task.applicationID,
			task.taskID,
			task.application.partition,
			terminationType,
		)

		if releaseRequest.Releases != nil {
			log.Log(log.ShimCacheTask).Info("releasing allocations",
				zap.Int("numOfAllocationsToRelease", len(releaseRequest.Releases.AllocationsToRelease)))
		}
		if err := task.context.apiProvider.GetAPIs().SchedulerAPI.UpdateAllocation(releaseRequest); err != nil {
			log.Log(log.ShimCacheTask).Debug("failed to send scheduling request to scheduler", zap.Error(err))
		}
	}
}

// shouldAppRelease reports whether the release of this task is deferred until
// the application is accepted by the core (YUNIKORN-3089). It is called from
// the task-release path with the task lock held (see releaseAllocation); it
// must not drop the task lock and must not acquire the application lock
// (tryAddReleasableTask uses a dedicated leaf lock instead).
func (task *Task) shouldAppRelease() bool {
	return task.application.tryAddReleasableTask(task)
}

// some sanity checks before sending task for scheduling,
// this reduces the scheduling overhead by blocking such
// request away from the core scheduler.
func (task *Task) sanityCheckBeforeScheduling() error {
	return task.checkPodPVCs()
}

// throw a warning if the pod has inconsistent metadata
func (task *Task) checkPodMetadataBeforeScheduling() {
	appID := utils.GetApplicationIDFromPod(task.pod)
	ignoredAppIDLabels, ignoredAppIDAnnotation := utils.GetIgnoredLabelAnnotationInPod(task.pod, appID, constants.AppIdLabelKeys, constants.AppIdAnnotationKeys)
	if len(ignoredAppIDLabels) > 0 || len(ignoredAppIDAnnotation) > 0 {
		task.logIgnoredPodMetadata("app-id", appID, ignoredAppIDLabels, ignoredAppIDAnnotation)
	}

	queueName := utils.GetQueueNameFromPod(task.pod)
	ignoredQueueLabels, ignoredQueueAnnotation := utils.GetIgnoredLabelAnnotationInPod(task.pod, queueName, constants.QueueLabelKeys, constants.QueueAnnotationKeys)
	if len(ignoredQueueLabels) > 0 || len(ignoredQueueAnnotation) > 0 {
		task.logIgnoredPodMetadata("queue", queueName, ignoredQueueLabels, ignoredQueueAnnotation)
	}
}

func (task *Task) logIgnoredPodMetadata(metadataType string, fianlValue string, ignoredLabel map[string]string, ignoredAnnotation map[string]string) {
	ignoredItems := make([]string, 0)
	for key, value := range ignoredLabel {
		ignoredItems = append(ignoredItems, fmt.Sprintf("(Label) %s: %s", key, value))
	}
	for key, value := range ignoredAnnotation {
		ignoredItems = append(ignoredItems, fmt.Sprintf("(Annotation) %s: %s", key, value))
	}
	logMessage := fmt.Sprintf("Found multiple '%s' value in pod. { podName: %s, fianlValue: %s, ignored: [%s] }",
		metadataType, task.pod.Name, fianlValue, strings.Join(ignoredItems, ", "))

	log.Log(log.ShimCacheTask).Warn(logMessage)
	events.GetRecorder().Eventf(task.pod.DeepCopy(),
		nil, v1.EventTypeWarning, "Scheduling", "Scheduling", logMessage)
}

func (task *Task) checkPodPVCs() error {
	task.lock.RLock()
	// Check PVCs used by the pod
	namespace := task.pod.Namespace
	manifest := &(task.pod.Spec)
	task.lock.RUnlock()
	for i := range manifest.Volumes {
		volume := &manifest.Volumes[i]
		if volume.PersistentVolumeClaim == nil {
			// Volume is not a PVC, ignore
			continue
		}
		pvcName := volume.PersistentVolumeClaim.ClaimName
		log.Log(log.ShimCacheTask).Debug("checking PVC", zap.String("name", pvcName))
		pvc, err := task.context.apiProvider.GetAPIs().PVCInformer.Lister().PersistentVolumeClaims(namespace).Get(pvcName)
		if err != nil {
			return err
		}
		if pvc.DeletionTimestamp != nil {
			return fmt.Errorf("persistentvolumeclaim %q is being deleted", pvc.Name)
		}
	}
	return nil
}

func (task *Task) UpdatePodCondition(podCondition *v1.PodCondition) (bool, *v1.Pod) {
	task.lock.Lock()
	defer task.lock.Unlock()

	status := task.podStatus.DeepCopy()
	pod := task.pod.DeepCopy()
	pod.Status = *status
	if !utils.PodUnderCondition(pod, podCondition) {
		log.Log(log.ShimContext).Debug("updating pod condition",
			zap.String("namespace", task.pod.Namespace),
			zap.String("name", task.pod.Name),
			zap.Any("podCondition", podCondition))
		if podutil.UpdatePodCondition(&task.podStatus, podCondition) {
			status = task.podStatus.DeepCopy()
			pod.Status = *status
			return true, pod
		}
	}

	return false, pod
}

func (task *Task) GetAllocationKey() string {
	task.lock.RLock()
	defer task.lock.RUnlock()
	return task.allocationKey
}

func (task *Task) setAllocationKey(allocationKey string) {
	task.lock.Lock()
	defer task.lock.Unlock()
	task.allocationKey = allocationKey
}

func (task *Task) FailWithEvent(errorMessage, actionReason string) {
	task.lock.RLock()
	defer task.lock.RUnlock()
	task.failWithEvent(errorMessage, actionReason)
}

func (task *Task) failWithEvent(errorMessage, actionReason string) {
	events.GetRecorder().Eventf(task.pod.DeepCopy(),
		nil, v1.EventTypeWarning, actionReason, actionReason, errorMessage)
	dispatcher.Dispatch(NewFailTaskEvent(task.applicationID, task.taskID, errorMessage))
}

func (task *Task) SetTaskPod(pod *v1.Pod) {
	task.lock.Lock()
	defer task.lock.Unlock()

	task.pod = pod
	oldResource := task.resource
	newResource := common.GetPodResource(pod)
	if !common.Equals(oldResource, newResource) {
		// pod resources have changed
		task.resource = newResource

		// update allocation in core
		task.updateAllocation()
	}
}
