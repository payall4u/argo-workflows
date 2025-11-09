package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/argoproj/argo-workflows/v3/util/logging"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/workflow/util"
)

var (
	namespace  string
	configPath string
)

func main() {
	cmd := &cobra.Command{
		Use:   "workflow-informer",
		Short: "Monitor Argo Workflow status changes and track push times",
		Run:   run,
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "Namespace to monitor (default: all namespaces)")
	cmd.Flags().StringVarP(&configPath, "kubeconfig", "k", "", "Path to kubeconfig file")

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	config, err := loadKubeConfig(configPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	dynamicClient, err := dynamic.NewForConfig(config)

	if err != nil {
		log.Fatal(err.Error())
	}
	// Create workflow informer
	informer := createWorkflowInformer(ctx, dynamicClient, namespace)

	// Setup event handlers
	addEventHandlers(ctx, informer)

	informer.Run(wait.NeverStop)
}

func loadKubeConfig(configPath string) (*rest.Config, error) {
	if configPath != "" {
		return clientcmd.BuildConfigFromFlags("", configPath)
	}
	return rest.InClusterConfig()
}

func createWorkflowInformer(ctx context.Context, dynamicClient dynamic.Interface, namespace string) cache.SharedIndexInformer {
	informer := util.NewWorkflowInformer(
		ctx,
		dynamicClient,
		namespace,
		time.Hour,
		nil, // tweakListRequestListOptions
		nil, // tweakWatchRequestListOptions
		cache.Indexers{},
	)

	return informer
}

func addEventHandlers(ctx context.Context, informer cache.SharedIndexInformer) {

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			un, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Printf("Received non-unstructured object in AddFunc")
				return
			}

			wf, err := util.FromUnstructured(un)
			if err != nil {
				log.Fatalf("Failed to convert unstructured to workflow: %s", err.Error())
				return
			}

			logWorkflowEvent(ctx, "ADDED", wf, time.Now())
		},

		UpdateFunc: func(oldObj, newObj interface{}) {
			oldUn, _ := oldObj.(*unstructured.Unstructured)
			newUn, _ := newObj.(*unstructured.Unstructured)
			// Skip if resource version hasn't changed
			if oldUn.GetResourceVersion() == newUn.GetResourceVersion() {
				return
			}

			newWf, err := util.FromUnstructured(newUn)
			if err != nil {
				return
			}

			logWorkflowEvent(ctx, "UPDATED", newWf, time.Now())
		},

		DeleteFunc: func(obj interface{}) {
			un, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}

			wf, err := util.FromUnstructured(un)
			if err != nil {
				return
			}

			logWorkflowEvent(ctx, "DELETED", wf, time.Now())
		},
	})

	if err != nil {
		log.Fatal(err.Error())
	}
}

func logWorkflowEvent(ctx context.Context, eventType string, wf *wfv1.Workflow, timestamp time.Time) {

	fields := logging.Fields{
		"event":      eventType,
		"namespace":  wf.Namespace,
		"workflow":   wf.Name,
		"phase":      string(wf.Status.Phase),
		"timestamp":  timestamp.Format(time.RFC3339Nano),
		"generation": wf.Generation,
		"uid":        string(wf.UID),
	}

	// Add duration information if available
	if !wf.Status.StartedAt.IsZero() {
		fields["startedAt"] = wf.Status.StartedAt.Format(time.RFC3339Nano)
		if !wf.Status.FinishedAt.IsZero() {
			fields["finishedAt"] = wf.Status.FinishedAt.Format(time.RFC3339Nano)
			fields["duration"] = wf.Status.FinishedAt.Sub(wf.Status.StartedAt.Time).String()
		} else {
			fields["duration"] = time.Since(wf.Status.StartedAt.Time).String()
		}
	}

	// Add message if available
	if wf.Status.Message != "" {
		fields["message"] = wf.Status.Message
	}

	// Add node count information
	if len(wf.Status.Nodes) > 0 {
		fields["nodeCount"] = len(wf.Status.Nodes)

		// Count nodes by phase
		phaseCounts := make(map[wfv1.NodePhase]int)
		for _, node := range wf.Status.Nodes {
			phaseCounts[node.Phase]++
		}
		fields["nodePhases"] = phaseCounts
	}

	fmt.Printf("%s %s %s\n", time.Now().String(), eventType, fieldsToString(fields))
}

func fieldsToString(fields logging.Fields) string {
	var sb strings.Builder
	for k, v := range fields {
		sb.WriteString(fmt.Sprintf("%s=%v ", k, v))
	}
	return sb.String()
}
