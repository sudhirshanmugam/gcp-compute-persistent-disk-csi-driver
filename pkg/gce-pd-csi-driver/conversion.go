/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gceGCEDriver

// Disk type conversion runs for far longer than a single CSI call may block,
// and the driver is only invoked on CSI lifecycle events, so it cannot rely on
// in-memory state to track a conversion across controller restarts or leader
// election. Instead the state of a conversion lives in annotations on the
// PersistentVolume:
//
//	pdcsi.gke.io/disk-type-conversion-operation
//	    "Pending"  the volume is marked for conversion but it has not started,
//	               because the disk is still attached
//	    <selfLink> the self link of the running disks.convert operation
//	    absent     no conversion is outstanding
//
//	pdcsi.gke.io/disk-type-converted-from
//	pdcsi.gke.io/disk-type-converted-to
//	    recorded once a conversion has completed
//
// ControllerModifyVolume drives the conversion forward, and the attach path
// falls back to finishing the bookkeeping so that a conversion which completed
// while no CSI call was in flight is still recorded.

import (
	"context"
	"fmt"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/k8sclient"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
)

// operationStatusDone is the status a finished GCE operation reports.
const operationStatusDone = "DONE"

const (
	// ConversionOperationAnnotation holds the self link of the running
	// conversion operation, or ConversionPending.
	ConversionOperationAnnotation = "pdcsi.gke.io/disk-type-conversion-operation"
	// ConvertedFromAnnotation holds the disk type before a completed conversion.
	ConvertedFromAnnotation = "pdcsi.gke.io/disk-type-converted-from"
	// ConvertedToAnnotation holds the disk type after a completed conversion.
	ConvertedToAnnotation = "pdcsi.gke.io/disk-type-converted-to"

	// ConversionPending marks a volume whose conversion cannot start yet
	// because the disk is still attached.
	ConversionPending = "Pending"
)

// conversionState is the outstanding conversion recorded on a PersistentVolume.
type conversionState struct {
	// Pending is true when the volume is marked for conversion but the
	// conversion has not been started yet.
	Pending bool
	// OperationSelfLink identifies a started conversion operation. Empty when
	// the conversion is only marked as pending.
	OperationSelfLink string
}

// Marked reports whether any conversion is outstanding for the volume.
func (c conversionState) Marked() bool {
	return c.Pending || c.OperationSelfLink != ""
}

// getConversionState reads the conversion annotation for the volume. A volume
// with no annotation, or a PV that cannot be read, reports no outstanding
// conversion so that unrelated operations are never blocked by bookkeeping.
func getConversionState(ctx context.Context, pvName string) (conversionState, error) {
	if pvName == "" {
		return conversionState{}, nil
	}
	pv, err := k8sclient.GetPersistentVolumeWithRetry(ctx, pvName)
	if err != nil {
		return conversionState{}, fmt.Errorf("failed to get PersistentVolume %s: %w", pvName, err)
	}
	value := pv.GetAnnotations()[ConversionOperationAnnotation]
	switch value {
	case "":
		return conversionState{}, nil
	case ConversionPending:
		return conversionState{Pending: true}, nil
	default:
		return conversionState{OperationSelfLink: value}, nil
	}
}

// getConversionTargetParameters returns the parameters of the
// VolumeAttributesClass currently requested for the volume. Detach carries no
// VolumeAttributesClass, so the target configuration of a pending conversion
// has to be read back from the PersistentVolume and its class. A volume with no
// class requested returns no parameters.
func getConversionTargetParameters(ctx context.Context, pvName string) (parameters.ModifyVolumeParameters, bool, error) {
	pv, err := k8sclient.GetPersistentVolumeWithRetry(ctx, pvName)
	if err != nil {
		return parameters.ModifyVolumeParameters{}, false, fmt.Errorf("failed to get PersistentVolume %s: %w", pvName, err)
	}
	if pv.Spec.VolumeAttributesClassName == nil || *pv.Spec.VolumeAttributesClassName == "" {
		return parameters.ModifyVolumeParameters{}, false, nil
	}
	vacParams, err := k8sclient.GetVolumeAttributesClassParameters(ctx, *pv.Spec.VolumeAttributesClassName)
	if err != nil {
		return parameters.ModifyVolumeParameters{}, false, err
	}
	modifyParams, err := parameters.ExtractModifyVolumeParameters(vacParams)
	if err != nil {
		return parameters.ModifyVolumeParameters{}, false, fmt.Errorf("failed to read parameters of VolumeAttributesClass %s: %w", *pv.Spec.VolumeAttributesClassName, err)
	}
	return modifyParams, true, nil
}

// markConversionPending records that the volume should be converted once its
// disk is detached.
func markConversionPending(ctx context.Context, pvName string) error {
	return setConversionAnnotations(ctx, pvName, map[string]*string{
		ConversionOperationAnnotation: strPtr(ConversionPending),
	})
}

// markConversionStarted records the operation that is carrying out the
// conversion, replacing any pending marker.
func markConversionStarted(ctx context.Context, pvName, operationSelfLink string) error {
	return setConversionAnnotations(ctx, pvName, map[string]*string{
		ConversionOperationAnnotation: strPtr(operationSelfLink),
	})
}

// markConversionComplete records the type change and clears the operation, so
// that the volume is no longer considered to be converting.
func markConversionComplete(ctx context.Context, pvName, fromDiskType, toDiskType string) error {
	return setConversionAnnotations(ctx, pvName, map[string]*string{
		ConversionOperationAnnotation: nil,
		ConvertedFromAnnotation:       strPtr(fromDiskType),
		ConvertedToAnnotation:         strPtr(toDiskType),
	})
}

// clearConversionMark removes an outstanding conversion marker, used when the
// volume is no longer marked for conversion.
func clearConversionMark(ctx context.Context, pvName string) error {
	return setConversionAnnotations(ctx, pvName, map[string]*string{
		ConversionOperationAnnotation: nil,
	})
}

func setConversionAnnotations(ctx context.Context, pvName string, annotations map[string]*string) error {
	if pvName == "" {
		return fmt.Errorf("cannot record conversion state without a PersistentVolume name")
	}
	if err := k8sclient.PatchPersistentVolumeAnnotations(ctx, pvName, annotations); err != nil {
		return fmt.Errorf("failed to record conversion state on PersistentVolume %s: %w", pvName, err)
	}
	klog.V(4).Infof("Recorded conversion state on PersistentVolume %s: %v", pvName, annotationKeys(annotations))
	return nil
}

// finishConversionIfMarked records a conversion as complete when the disk has
// reached the requested type. Bookkeeping failures are logged rather than
// returned: the conversion itself has already succeeded, and the annotations
// are refreshed again at the next attach.
func (gceCS *GCEControllerServer) finishConversionIfMarked(ctx context.Context, pvName, volumeID, currentDiskType string) {
	state, err := getConversionState(ctx, pvName)
	if err != nil {
		klog.Warningf("Could not read conversion state for volume %s: %v", volumeID, err)
		return
	}
	if !state.Marked() {
		return
	}
	klog.V(4).Infof("Conversion of volume %s to %s completed", volumeID, currentDiskType)
	if err := markConversionComplete(ctx, pvName, conversionSourceType(ctx, pvName), currentDiskType); err != nil {
		klog.Errorf("Failed to record completed conversion for volume %s: %v", volumeID, err)
	}
}

// checkConversionBeforeAttach blocks an attach while a conversion of the disk
// is running, and completes the bookkeeping of a conversion that has finished.
//
// This is the fallback the driver relies on: nothing polls a conversion between
// CSI calls, so a conversion that completed while the driver was idle, or was
// started by an instance that has since restarted, is reconciled here.
func (gceCS *GCEControllerServer) checkConversionBeforeAttach(ctx context.Context, pvName, volumeID string, disk *gce.CloudDisk) error {
	state, err := getConversionState(ctx, pvName)
	if err != nil {
		// Never block attach because the conversion state could not be read.
		klog.Warningf("Could not read conversion state for volume %s: %v", volumeID, err)
		return nil
	}
	if state.OperationSelfLink == "" {
		// Either nothing is outstanding, or the conversion is only marked as
		// pending, in which case it has not started and does not block attach.
		return nil
	}

	operationStatus, opErr := gceCS.CloudProvider.GetConvertDiskOperation(ctx, state.OperationSelfLink)
	if opErr != nil {
		klog.Errorf("Conversion operation for volume %s failed: %v", volumeID, opErr)
		// The conversion failed rather than being in flight, so it no longer
		// blocks attach. It stays marked so that it is retried.
		if operationStatus == operationStatusDone {
			return nil
		}
	}
	if operationStatus != operationStatusDone {
		return status.Errorf(codes.FailedPrecondition, "cannot attach volume %s because a disk type conversion is in progress", volumeID)
	}

	klog.V(4).Infof("Conversion of volume %s completed, recording result", volumeID)
	if err := markConversionComplete(ctx, pvName, conversionSourceType(ctx, pvName), disk.GetPDType()); err != nil {
		klog.Errorf("Failed to record completed conversion for volume %s: %v", volumeID, err)
	}
	return nil
}

// startPendingConversionAfterDetach starts a conversion that was waiting for the
// disk to be detached. Detach carries no VolumeAttributesClass, so the target
// configuration is read back from the volume's class.
//
// Errors are logged rather than returned: the detach itself succeeded, and the
// conversion is retried by the modification that is still outstanding.
func (gceCS *GCEControllerServer) startPendingConversionAfterDetach(ctx context.Context, project string, volKey *meta.Key, volumeID string) {
	state, err := getConversionState(ctx, volKey.Name)
	if err != nil || !state.Pending {
		return
	}

	params, found, err := getConversionTargetParameters(ctx, volKey.Name)
	if err != nil {
		klog.Warningf("Could not read conversion target for volume %s: %v", volumeID, err)
		return
	}
	// The class was removed while the conversion was pending, so the volume is
	// no longer marked for conversion.
	if !found || params.DiskType == nil {
		if err := clearConversionMark(ctx, volKey.Name); err != nil {
			klog.Errorf("Failed to clear conversion mark for volume %s: %v", volumeID, err)
		}
		return
	}

	disk, err := gceCS.CloudProvider.GetDisk(ctx, project, volKey)
	if err != nil {
		klog.Warningf("Could not get disk %s to start pending conversion: %v", volumeID, err)
		return
	}
	currentDiskType := disk.GetPDType()
	if *params.DiskType == currentDiskType {
		if err := clearConversionMark(ctx, volKey.Name); err != nil {
			klog.Errorf("Failed to clear conversion mark for volume %s: %v", volumeID, err)
		}
		return
	}
	if len(disk.GetUsers()) > 0 {
		// Still attached elsewhere, so the conversion has to keep waiting.
		return
	}

	if err := gceCS.startDiskConversion(ctx, project, volKey, volumeID, currentDiskType, *params.DiskType, params); err != nil {
		klog.Warningf("Could not start pending conversion for volume %s: %v", volumeID, err)
	}
}

// conversionSourceType returns the disk type recorded when the conversion
// started, which is the only place the original type survives.
func conversionSourceType(ctx context.Context, pvName string) string {
	pv, err := k8sclient.GetPersistentVolumeWithRetry(ctx, pvName)
	if err != nil {
		return ""
	}
	return pv.GetAnnotations()[ConvertedFromAnnotation]
}

// recordConversionStartedEvent emits a Kubernetes event against the
// PersistentVolume so that the start of a conversion is visible to users, who
// otherwise only see a modification that stays in progress. Failure to emit an
// event never fails the conversion.
func (gceCS *GCEControllerServer) recordConversionStartedEvent(ctx context.Context, pvName, fromDiskType, toDiskType string) {
	if pvName == "" {
		return
	}
	pv, err := k8sclient.GetPersistentVolumeWithRetry(ctx, pvName)
	if err != nil {
		klog.Warningf("Could not emit conversion event for PersistentVolume %s: %v", pvName, err)
		return
	}
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pvName + ".",
			Namespace:    metav1.NamespaceDefault,
		},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "PersistentVolume",
			Name:       pv.Name,
			UID:        pv.UID,
		},
		Reason:  "DiskTypeConversionStart",
		Message: fmt.Sprintf("Disk type conversion started for volume %q from %s to %s", pvName, fromDiskType, toDiskType),
		Action:  "ConvertDiskType",
		Type:    corev1.EventTypeNormal,
		Source: corev1.EventSource{
			Component: "pdcsi",
		},
		ReportingController: "pdcsi",
		FirstTimestamp:      metav1.Now(),
		LastTimestamp:       metav1.Now(),
		Count:               1,
	}
	if err := k8sclient.CreateEvent(ctx, event); err != nil {
		klog.Warningf("Could not emit conversion event for PersistentVolume %s: %v", pvName, err)
	}
}

func annotationKeys(annotations map[string]*string) []string {
	keys := make([]string, 0, len(annotations))
	for key := range annotations {
		keys = append(keys, key)
	}
	return keys
}

func strPtr(s string) *string {
	return &s
}
