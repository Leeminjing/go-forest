package temporalforest

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Named private fields deliberately do not promote Start/Stop or Nexus APIs.
// Merely converting a native Worker to Registry would still expose its actuator
// through a type assertion inside a preparation callback.
type registry struct {
	workflows  worker.WorkflowRegistry
	activities worker.ActivityRegistry
}

func (r registry) RegisterWorkflow(f any) { r.workflows.RegisterWorkflow(f) }
func (r registry) RegisterWorkflowWithOptions(f any, o workflow.RegisterOptions) {
	r.workflows.RegisterWorkflowWithOptions(f, o)
}
func (r registry) RegisterDynamicWorkflow(f any, o workflow.DynamicRegisterOptions) {
	r.workflows.RegisterDynamicWorkflow(f, o)
}
func (r registry) RegisterActivity(f any) { r.activities.RegisterActivity(f) }
func (r registry) RegisterActivityWithOptions(f any, o activity.RegisterOptions) {
	r.activities.RegisterActivityWithOptions(f, o)
}
func (r registry) RegisterDynamicActivity(f any, o activity.DynamicRegisterOptions) {
	r.activities.RegisterDynamicActivity(f, o)
}
