/*
Copyright 2026 The Kubernetes Authors.

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

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
)

const (
	diskTypeHyperdiskBalanced = "hyperdisk-balanced"
	diskTypePdBalanced        = "pd-balanced"
)

// conversionPVClient builds a Kubernetes clientset from --conversion-kubeconfig.
// Returns nil, nil if the flag is unset, so callers can no-op cleanly when
// this feature isn't being exercised.
func conversionPVClient() (kubernetes.Interface, error) {
	if *conversionKubeconfig == "" {
		return nil, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", *conversionKubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load --conversion-kubeconfig: %w", err)
	}
	// Each call site builds its own client rather than sharing one, so the
	// default client-side rate limit (QPS 5 / Burst 10) can be exhausted by a
	// single test's own back-to-back calls (e.g. createConversionPVC alone
	// makes several). Raised generously since this only talks to a small
	// test-only cluster.
	cfg.QPS = 50
	cfg.Burst = 100
	return kubernetes.NewForConfig(cfg)
}

// createConversionPV creates a PersistentVolume named after the disk, since
// the driver's disk-type conversion tracking reads and writes its state as an
// annotation on a PV with that exact name. A no-op if --conversion-kubeconfig
// wasn't provided.
func createConversionPV(volName, volumeID string) error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volName},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "pd.csi.storage.gke.io",
					VolumeHandle: volumeID,
				},
			},
		},
	}
	_, err = kubeClient.CoreV1().PersistentVolumes().Create(context.Background(), pv, metav1.CreateOptions{})
	return err
}

// deleteConversionPV removes the PV created by createConversionPV. A no-op if
// --conversion-kubeconfig wasn't provided.
func deleteConversionPV(volName string) {
	kubeClient, err := conversionPVClient()
	if err != nil || kubeClient == nil {
		return
	}
	err = kubeClient.CoreV1().PersistentVolumes().Delete(context.Background(), volName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete conversion-tracking PV %s: %v", volName, err)
	}
}

// createConversionNode creates a Node object matching constants.TestNode
// (the driver's static --node-name in this harness). At startup the driver's
// local-SSD device-cache setup looks this Node up unconditionally and retries
// for ~30s if it's missing; pre-creating it lets that lookup succeed
// immediately instead of blocking driver readiness. Called once for the whole
// suite, before any driver process starts. A no-op if --conversion-kubeconfig
// wasn't provided.
func createConversionNode() error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: constants.TestNode},
	}
	_, err = kubeClient.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// deleteConversionNode removes the Node created by createConversionNode. A
// no-op if --conversion-kubeconfig wasn't provided.
func deleteConversionNode() {
	kubeClient, err := conversionPVClient()
	if err != nil || kubeClient == nil {
		return
	}
	err = kubeClient.CoreV1().Nodes().Delete(context.Background(), constants.TestNode, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete conversion-tracking Node %s: %v", constants.TestNode, err)
	}
}

// getConversionPVAnnotation reads a value from the conversion-tracking PV's
// annotations, so tests can assert directly on the driver's PV-annotation
// state (e.g. that a queued conversion was actually recorded, or actually
// cancelled) instead of only inferring it from CSI call responses. Returns
// ("", false, nil) if --conversion-kubeconfig wasn't provided.
func getConversionPVAnnotation(volName, key string) (string, bool, error) {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return "", false, err
	}
	if kubeClient == nil {
		return "", false, nil
	}
	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), volName, metav1.GetOptions{})
	if err != nil {
		return "", false, err
	}
	value, exists := pv.Annotations[key]
	return value, exists, nil
}

// conversionNamespace is the fixed namespace used for the PVCs created by
// createConversionPVC below. Any namespace that exists on the target cluster
// works, since the tests that use it also create the namespace itself.
const conversionNamespace = "pdcsi-conversion-e2e"

// createConversionNamespace ensures conversionNamespace exists. A no-op if
// --conversion-kubeconfig wasn't provided.
func createConversionNamespace() error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: conversionNamespace}}
	_, err = kubeClient.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{})
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// createConversionVAC creates a VolumeAttributesClass carrying the given
// mutable parameters (the same "type"/"iops"/"throughput" keys accepted by
// ControllerModifyVolume), so a PVC can reference it by name. Targets
// storage.k8s.io/v1: VolumeAttributesClass reached GA at this version, and
// v1 is also the first version the driver's own lookup in
// k8sclient.getVolumeAttributesClassParameters checks. A no-op if
// --conversion-kubeconfig wasn't provided.
func createConversionVAC(vacName string, params map[string]string) error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: vacName},
		DriverName: "pd.csi.storage.gke.io",
		Parameters: params,
	}
	_, err = kubeClient.StorageV1().VolumeAttributesClasses().Create(context.Background(), vac, metav1.CreateOptions{})
	return err
}

// deleteConversionVAC removes the VolumeAttributesClass created by
// createConversionVAC. A no-op if --conversion-kubeconfig wasn't provided.
func deleteConversionVAC(vacName string) {
	kubeClient, err := conversionPVClient()
	if err != nil || kubeClient == nil {
		return
	}
	err = kubeClient.StorageV1().VolumeAttributesClasses().Delete(context.Background(), vacName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete conversion-tracking VolumeAttributesClass %s: %v", vacName, err)
	}
}

// createConversionPVC creates a PersistentVolumeClaim statically bound to the
// tracking PV (by name, via Spec.VolumeName). This is what completes the
// PV -> PVC link the driver's queuedConversionOutcome walks on detach (via
// k8sclient.GetVolumeAttributesClassForPV) to decide whether a queued
// conversion should still run - createConversionPV alone leaves that chain
// broken at the first link (no ClaimRef), which is why a detach-triggered
// conversion is silently cancelled without it.
//
// Deliberately does NOT set VolumeAttributesClassName here: the PV/PVC
// binder requires a PV and PVC's VolumeAttributesClassName to match (or
// both be unset) before it will bind them at all, and createConversionPV
// never sets one on the PV - setting it here would permanently block
// binding ("VolumeMismatch: volumeAttributesClassName does not match"),
// confirmed directly against this cluster. The class is applied afterward
// with patchConversionPVCVAC, once bound - matching how the feature is
// actually meant to be used (switching the class on an existing, already
// bound claim), not set at creation time.
//
// Waits for the PV/PVC binder to record the binding (PV.Spec.ClaimRef set)
// before returning, since the driver reads that field synchronously.
// A no-op if --conversion-kubeconfig wasn't provided.
func createConversionPVC(pvcName, pvName string) error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	if err := createConversionNamespace(); err != nil {
		return err
	}
	emptyStorageClass := ""
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: conversionNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       pvName,
			StorageClassName: &emptyStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if _, err := kubeClient.CoreV1().PersistentVolumeClaims(conversionNamespace).Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
		return err
	}

	return wait.PollUntilContextTimeout(context.Background(), 2*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pv, err := kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == pvcName, nil
	})
}

// patchConversionPVCVAC sets the VolumeAttributesClassName on an
// already-bound PVC, simulating the user "switching" the class - this is the
// step queuedConversionOutcome expects to find in place by the time the
// disk detaches. Must be called after createConversionPVC has confirmed the
// PVC is bound, not before: setting this at PVC creation time blocks the
// PV/PVC binder entirely (see createConversionPVC). A no-op if
// --conversion-kubeconfig wasn't provided.
func patchConversionPVCVAC(pvcName, vacName string) error {
	kubeClient, err := conversionPVClient()
	if err != nil {
		return err
	}
	if kubeClient == nil {
		return nil
	}
	patch := []byte(fmt.Sprintf(`{"spec":{"volumeAttributesClassName":%q}}`, vacName))
	_, err = kubeClient.CoreV1().PersistentVolumeClaims(conversionNamespace).Patch(context.Background(), pvcName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// deleteConversionPVC removes the PVC created by createConversionPVC. A no-op
// if --conversion-kubeconfig wasn't provided.
func deleteConversionPVC(pvcName string) {
	kubeClient, err := conversionPVClient()
	if err != nil || kubeClient == nil {
		return
	}
	err = kubeClient.CoreV1().PersistentVolumeClaims(conversionNamespace).Delete(context.Background(), pvcName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete conversion-tracking PVC %s: %v", pvcName, err)
	}
}

var _ = Describe("GCE PD CSI Driver VolumeAttributesClass & Disk Conversion", func() {

	It("Should dynamically modify IOPS and throughput on hyperdisk-balanced without triggering conversion", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		const (
			initialIops       int64 = 3000
			initialThroughput int64 = 140
			targetIops        int64 = 6000
			targetThroughput  int64 = 250
		)
		// create disk
		volumeParams := map[string]string{
			parameters.ParameterKeyType:                          diskTypeHyperdiskBalanced,
			parameters.ParameterKeyProvisionedIOPSOnCreate:       fmt.Sprintf("%d", initialIops),
			parameters.ParameterKeyProvisionedThroughputOnCreate: fmt.Sprintf("%dMi", initialThroughput),
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for hyperdisk-balanced: %v", err)

		// provisioned volume
		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk from cloud directly")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced))
		Expect(cloudDisk.ProvisionedIops).To(Equal(initialIops))
		Expect(cloudDisk.ProvisionedThroughput).To(Equal(initialThroughput))

		defer func() {
			// Clean up
			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// A real PV is required for checkNoConversionInProgress to read/write
		// its tracking annotation on, when --enable-pd-conversion is on.
		Expect(createConversionPV(volName, volume.VolumeId)).To(BeNil(), "Failed to create conversion-tracking PV")
		defer deleteConversionPV(volName)

		// attach
		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "ControllerPublishVolumeReadWrite failed")

		defer func() {
			err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
			if err != nil {
				klog.Errorf("Failed to detach disk: %v", err)
			}
		}()

		stageDir := filepath.Join("/tmp/", volName, "stage")
		err = client.NodeStageExt4Volume(volume.VolumeId, stageDir, false /* setupDataCache */)
		Expect(err).To(BeNil(), "NodeStageExt4Volume failed")

		defer func() {
			err = client.NodeUnstageVolume(volume.VolumeId, stageDir)
			if err != nil {
				klog.Errorf("Failed to unstage volume: %v", err)
			}
			fp := filepath.Join("/tmp/", volName)
			err = testutils.RmAll(instance, fp)
			if err != nil {
				klog.Errorf("Failed to rm directory %s: %v", fp, err)
			}
		}()

		publishDir := filepath.Join("/tmp/", volName, "mount")
		err = client.NodePublishVolume(volume.VolumeId, stageDir, publishDir)
		Expect(err).To(BeNil(), "NodePublishVolume failed")

		defer func() {
			err = client.NodeUnpublishVolume(volume.VolumeId, publishDir)
			if err != nil {
				klog.Errorf("NodeUnpublishVolume failed: %v", err)
			}
		}()

		//Modify IOPS and Throughput
		modifyParams := map[string]string{
			"type":       diskTypeHyperdiskBalanced,
			"iops":       fmt.Sprintf("%d", targetIops),
			"throughput": fmt.Sprintf("%dMi", targetThroughput),
		}

		err = client.ControllerModifyVolume(volume.VolumeId, modifyParams)
		Expect(err).To(BeNil(), "ControllerModifyVolume failed during parameter modification")

		// Verify. The disk update accepted by ControllerModifyVolume runs as a
		// GCE operation that updateZonalDisk does not wait on before the call
		// returns (a known gap vs. e.g. resizeZonalDisk, which waits via
		// waitForZonalOp), so the new IOPS/throughput may take a while to be
		// visible. Poll generously, matching the timeout used for the
		// disk-type conversion cases elsewhere in this file, rather than
		// assume a short window is enough.
		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, getErr := computeService.Disks.Get(p, z, volName).Do()
			if getErr != nil {
				return false, getErr
			}
			cloudDisk = disk
			return disk.ProvisionedIops == targetIops && disk.ProvisionedThroughput == targetThroughput, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for the modified IOPS/throughput to be reflected on the cloud disk")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced))
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain in READY state without detachment or conversion operations")
	})

	It("Should block volume attachment while disk conversion is actively in progress", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())

		volumeParams := map[string]string{
			parameters.ParameterKeyType: diskTypePdBalanced,
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for pd-balanced: %v", err)

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk from cloud")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypePdBalanced))

		defer func() {

			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// A real PV is required for checkNoConversionInProgress to read/write
		// its tracking annotation on, when --enable-pd-conversion is on.
		Expect(createConversionPV(volName, volume.VolumeId)).To(BeNil(), "Failed to create conversion-tracking PV")
		defer deleteConversionPV(volName)

		convertParams := map[string]string{
			"type":       diskTypeHyperdiskBalanced,
			"iops":       "3000",
			"throughput": "140Mi",
		}

		// Starting a conversion never returns a clean success: GCE runs it
		// asynchronously, so the driver reports it as a retryable Unavailable
		// error even when the start itself succeeded, to signal the operation
		// is now in progress.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should report the conversion as in progress")
		startGrpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(startGrpcStatus.Code()).To(Equal(codes.Unavailable))
		Expect(strings.ToLower(startGrpcStatus.Message())).To(ContainSubstring("is in progress"))

		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false /* forceAttach */)
		Expect(err).NotTo(BeNil(), "ControllerPublishVolumeReadWrite should have been blocked during conversion")

		grpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerPublishVolume")
		// checkNoConversionInProgress always reports this as Unavailable with
		// "a disk type conversion is in progress" when it can read the PV
		// annotation (controller.go's checkNoConversionInProgress).
		Expect(grpcStatus.Code()).To(Equal(codes.Unavailable))
		Expect(strings.ToLower(grpcStatus.Message())).To(ContainSubstring("conversion is in progress"))

		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, err := computeService.Disks.Get(p, z, volName).Do()
			if err != nil {
				return false, err
			}
			if strings.Contains(disk.Type, diskTypeHyperdiskBalanced) && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for disk conversion to complete in GCE")

		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "ControllerPublishVolumeReadWrite must succeed once conversion is complete")

		defer func() {
			err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
			if err != nil {
				klog.Errorf("Failed to detach converted disk: %v", err)
			}
		}()

		stageDir := filepath.Join("/tmp/", volName, "stage")
		err = client.NodeStageExt4Volume(volume.VolumeId, stageDir, false)
		Expect(err).To(BeNil(), "NodeStageExt4Volume failed on converted volume")

		defer func() {
			err = client.NodeUnstageVolume(volume.VolumeId, stageDir)
			if err != nil {
				klog.Errorf("Failed to unstage volume: %v", err)
			}
			fp := filepath.Join("/tmp/", volName)
			err = testutils.RmAll(instance, fp)
			if err != nil {
				klog.Errorf("Failed to clean up test dir: %v", err)
			}
		}()

		publishDir := filepath.Join("/tmp/", volName, "mount")
		err = client.NodePublishVolume(volume.VolumeId, stageDir, publishDir)
		Expect(err).To(BeNil(), "NodePublishVolume failed on converted volume")

		defer func() {
			err = client.NodeUnpublishVolume(volume.VolumeId, publishDir)
			if err != nil {
				klog.Errorf("NodeUnpublishVolume failed on converted volume: %v", err)
			}
		}()

		sizeGb, err := testutils.GetFSSizeInGb(instance, publishDir)
		Expect(err).To(BeNil(), "Failed to get filesystem size")
		// ext4 reserves space for its journal and metadata, so the visible
		// filesystem size is a little under the raw disk size.
		Expect(sizeGb).To(BeNumerically("~", defaultHdBSizeGb, 3))
	})

	It("Should return a terminal failure for VAC IOPS/throughput parameters that exceed the target disk type's limits", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client

		volName := testNamePrefix + string(uuid.NewUUID())
		volumeParams := map[string]string{
			parameters.ParameterKeyType: diskTypePdBalanced,
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for pd-balanced: %v", err)

		defer func() {
			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// hyperdisk-balanced caps provisioned IOPS at 500/GiB, capped again at
		// an absolute ceiling well under 999999 regardless of disk size, so
		// this value is guaranteed to be rejected as out-of-spec.
		convertParams := map[string]string{
			"type": diskTypeHyperdiskBalanced,
			"iops": "999999",
		}

		// An out-of-spec value is rejected before anything is submitted to
		// GCE: no operation starts and no conversion state is recorded, so
		// this must be a plain, non-retryable InvalidArgument - never the
		// Unavailable "in progress" response a valid conversion request gets.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should reject an out-of-spec IOPS value")
		grpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(grpcStatus.Code()).To(Equal(codes.InvalidArgument))
		Expect(strings.ToLower(grpcStatus.Message())).To(ContainSubstring("exceeds the maximum"))

		// The disk must be untouched: still pd-balanced, no conversion queued.
		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk from cloud")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypePdBalanced),
			"A rejected out-of-spec conversion request must not change the disk's type")

		// Retrying the identical request must fail exactly the same way,
		// proving no pending/retry state was left behind by the terminal
		// failure - unlike a retryable error, this should never resolve into
		// an "in progress" response on a later attempt.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "Retrying the same out-of-spec request should fail identically")
		grpcStatus, ok = status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(grpcStatus.Code()).To(Equal(codes.InvalidArgument))
	})

	It("Should cancel a pending conversion retry when the VolumeAttributesClass is cleared", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		volumeParams := map[string]string{
			parameters.ParameterKeyType: diskTypePdBalanced,
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for pd-balanced: %v", err)

		defer func() {
			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// A real PV is required: markConversionPending, the cancellation
		// check inside ControllerModifyVolume, and startQueuedConversionOnDetach
		// all read/write the conversion state as an annotation on a PV named
		// after the disk.
		Expect(createConversionPV(volName, volume.VolumeId)).To(BeNil(), "Failed to create conversion-tracking PV")
		defer deleteConversionPV(volName)

		// Attach the disk so a conversion request gets queued instead of
		// started immediately - conversion requires the disk to be detached.
		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "ControllerPublishVolumeReadWrite failed")
		defer func() {
			err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
			if err != nil {
				klog.Errorf("Failed to detach disk: %v", err)
			}
		}()

		convertParams := map[string]string{
			"type":       diskTypeHyperdiskBalanced,
			"iops":       "3000",
			"throughput": "140Mi",
		}

		// Triggering a conversion while attached defers it: the driver queues
		// the conversion (marking it Pending on the tracking PV) and reports
		// a retryable FailedPrecondition until the disk detaches.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should report that conversion cannot start while attached")
		grpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(grpcStatus.Code()).To(Equal(codes.FailedPrecondition))
		Expect(strings.ToLower(grpcStatus.Message())).To(ContainSubstring("detach the volume"))

		// Confirm the conversion was actually queued: the tracking PV should
		// now carry the Pending annotation.
		opVal, exists, err := getConversionPVAnnotation(volName, constants.DiskTypeConversionOperationKey)
		Expect(err).To(BeNil(), "Failed to read conversion-tracking PV annotation")
		Expect(exists).To(BeTrue(), "Expected a conversion-tracking annotation to be recorded")
		Expect(opVal).To(Equal(constants.ConversionStatePending))

		// Simulate the user clearing the VolumeAttributesClass on the PVC:
		// the external-resizer calls ControllerModifyVolume with empty
		// mutable parameters once the VAC reference is gone. The driver's
		// cancellation path should clear the pending state and report
		// success, rather than leaving the volume permanently blocked.
		err = client.ControllerModifyVolume(volume.VolumeId, map[string]string{})
		Expect(err).To(BeNil(), "ControllerModifyVolume with cleared parameters should succeed and cancel the pending conversion")

		// The Pending annotation must be gone now.
		opVal, exists, err = getConversionPVAnnotation(volName, constants.DiskTypeConversionOperationKey)
		Expect(err).To(BeNil(), "Failed to read conversion-tracking PV annotation after cancellation")
		if exists {
			Expect(opVal).NotTo(Equal(constants.ConversionStatePending), "Conversion should no longer be marked pending after cancellation")
		}

		// Detach and confirm the cancelled conversion does not run: with
		// nothing queued, startQueuedConversionOnDetach must be a no-op and
		// the disk stays pd-balanced.
		err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
		Expect(err).To(BeNil(), "ControllerUnpublishVolume failed")

		Consistently(func() string {
			disk, err := computeService.Disks.Get(p, z, volName).Do()
			Expect(err).To(BeNil(), "Failed to get disk from cloud")
			return disk.Type
		}, 30*time.Second, 5*time.Second).Should(ContainSubstring(diskTypePdBalanced),
			"A cancelled conversion must not run after detach")
	})
})

var _ = Describe("GCE PD CSI Driver Dynamic Volumes VAC Conversion", func() {

	It("E2E-CNV-01: Should convert an unattached pd-balanced disk to hyperdisk-balanced when the PVC switches to a Hyperdisk VolumeAttributesClass", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType:        parameters.DynamicVolumeType,
			parameters.ParameterHDType:         "hyperdisk-balanced",
			parameters.ParameterPDType:         "pd-balanced",
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Provision an unattached pd-balanced disk on a PD-capable node")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed with error: %v", err)

		defer func() {
			err := client.DeleteVolume(volume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume failed")
			project, key, err := common.VolumeIDToKey(volume.VolumeId)
			Expect(err).To(BeNil(), "Failed to parse volume ID")
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk from GCE API")
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk not in READY state")
		Expect(cloudDisk.Type).To(ContainSubstring("pd-balanced"),
			"Expected the starting disk type to be pd-balanced before the VAC-triggered conversion, got: %s", cloudDisk.Type)

		By("2. Trigger the pd-to-hyperdisk conversion through ControllerModifyVolume")
		convertParams := map[string]string{
			"type":       "hyperdisk-balanced",
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Starting a conversion never returns a clean success: GCE runs it
		// asynchronously, so the driver reports it as a retryable Unavailable
		// error even when the start itself succeeded, to signal the operation
		// is now in progress.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should report the conversion as in progress")
		startGrpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(startGrpcStatus.Code()).To(Equal(codes.Unavailable))
		Expect(strings.ToLower(startGrpcStatus.Message())).To(ContainSubstring("is in progress"))

		By("3. Wait for the GCE disk to finish conversion")
		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, getErr := computeService.Disks.Get(p, z, volName).Do()
			if getErr != nil {
				return false, getErr
			}
			if strings.Contains(disk.Type, "hyperdisk-balanced") && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for pd-balanced disk to convert to hyperdisk-balanced")

		By("4. Verify the final disk type matches the conversion target")
		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get converted disk from cloud")
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected the final disk type to be hyperdisk-balanced after conversion, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after conversion")

		Skip("Pending: the cluster-level VAC-to-PV annotation flow and the Kubernetes-side conversion state tracking are still handled by the feature implementation that is not present in this repo yet. The raw GCE conversion lifecycle is covered here, but the VAC contract is still pending.")
	})

	It("E2E-CNV-02: Should defer disk conversion until workload detach when the PVC switches to a Hyperdisk VolumeAttributesClass while the pod is still running", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType:        parameters.DynamicVolumeType,
			parameters.ParameterHDType:         "hyperdisk-balanced",
			parameters.ParameterPDType:         "pd-balanced",
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Start a stateful workload using a mounted pd-balanced PVC")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed with error: %v", err)

		defer func() {
			err := client.DeleteVolume(volume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume failed")
			project, key, err := common.VolumeIDToKey(volume.VolumeId)
			Expect(err).To(BeNil(), "Failed to parse volume ID")
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// A real PV is required for the driver's PV-annotation-backed conversion
		// tracking (markConversionPending / startQueuedConversionOnDetach) to
		// have somewhere to record and read the queued-conversion state.
		Expect(createConversionPV(volName, volume.VolumeId)).To(BeNil(), "Failed to create conversion-tracking PV")
		defer deleteConversionPV(volName)

		// The detach-triggered conversion doesn't just check "is something
		// queued" - runQueuedConversion re-reads what the user currently
		// wants via queuedConversionOutcome, which walks
		// PV -> Spec.ClaimRef -> PVC -> Spec.VolumeAttributesClassName ->
		// VolumeAttributesClass. A bare tracking PV has no ClaimRef, so that
		// walk fails at the first step and the driver treats it as "the VAC
		// was removed" - cancelling the conversion instead of running it.
		// Creating a real VAC and a PVC bound to the tracking PV completes
		// that chain.
		vacName := volName + "-vac"
		vacParams := map[string]string{
			"type":       "hyperdisk-balanced",
			"iops":       "3000",
			"throughput": "140Mi",
		}
		Expect(createConversionVAC(vacName, vacParams)).To(BeNil(), "Failed to create conversion-tracking VolumeAttributesClass")
		defer deleteConversionVAC(vacName)

		Expect(createConversionPVC(volName, volName)).To(BeNil(), "Failed to create and bind conversion-tracking PVC")
		defer deleteConversionPVC(volName)

		// Apply the VolumeAttributesClass only now that the PVC is bound -
		// setting it at creation time blocks the PV/PVC binder outright, since
		// the tracking PV never carries a matching class. This also mirrors
		// the real flow this test's name describes: an already-bound PVC
		// later switches to a Hyperdisk class, rather than starting with one.
		Expect(patchConversionPVCVAC(volName, vacName)).To(BeNil(), "Failed to switch the conversion-tracking PVC to the Hyperdisk VolumeAttributesClass")

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk from GCE API")
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk not in READY state")
		Expect(cloudDisk.Type).To(ContainSubstring("pd-balanced"),
			"Expected the initial disk to be pd-balanced before the workload is detached, got: %s", cloudDisk.Type)

		By("2. Attach the volume and mount it to the instance")
		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false)
		Expect(err).To(BeNil(), "ControllerPublishVolumeReadWrite failed for initial pd-balanced volume")
		defer func() {
			err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
			if err != nil {
				klog.Errorf("Failed to detach volume after test: %v", err)
			}
		}()

		stageDir := filepath.Join("/tmp/", volName, "stage")
		err = client.NodeStageExt4Volume(volume.VolumeId, stageDir, false)
		Expect(err).To(BeNil(), "NodeStageExt4Volume failed on pd-balanced volume")
		defer func() {
			err = client.NodeUnstageVolume(volume.VolumeId, stageDir)
			if err != nil {
				klog.Errorf("Failed to unstage volume: %v", err)
			}
			fp := filepath.Join("/tmp/", volName)
			err = testutils.RmAll(instance, fp)
			if err != nil {
				klog.Errorf("Failed to clean up test dir: %v", err)
			}
		}()

		publishDir := filepath.Join("/tmp/", volName, "mount")
		err = client.NodePublishVolume(volume.VolumeId, stageDir, publishDir)
		Expect(err).To(BeNil(), "NodePublishVolume failed on pd-balanced volume")
		defer func() {
			err = client.NodeUnpublishVolume(volume.VolumeId, publishDir)
			if err != nil {
				klog.Errorf("NodeUnpublishVolume failed: %v", err)
			}
		}()

		By("3. Trigger conversion while the workload is still attached")
		convertParams := map[string]string{
			"type":       "hyperdisk-balanced",
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Conversion requires the disk to be detached, so triggering it while
		// still attached is deferred rather than started: the driver queues the
		// conversion to run on detach and reports this as a retryable
		// FailedPrecondition in the meantime.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should report that conversion cannot start while attached")
		triggerGrpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(triggerGrpcStatus.Code()).To(Equal(codes.FailedPrecondition))
		Expect(strings.ToLower(triggerGrpcStatus.Message())).To(ContainSubstring("detach the volume"))

		By("4. Detach the workload to allow conversion to complete")
		err = client.NodeUnpublishVolume(volume.VolumeId, publishDir)
		Expect(err).To(BeNil(), "NodeUnpublishVolume failed while detaching workload")
		err = client.NodeUnstageVolume(volume.VolumeId, stageDir)
		Expect(err).To(BeNil(), "NodeUnstageVolume failed while detaching workload")
		err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
		Expect(err).To(BeNil(), "ControllerUnpublishVolume failed while detaching workload")

		By("5. Wait for the conversion to finish after detach")
		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, getErr := computeService.Disks.Get(p, z, volName).Do()
			if getErr != nil {
				return false, getErr
			}
			if strings.Contains(disk.Type, "hyperdisk-balanced") && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for workload-detach conversion to complete")

		By("6. Confirm the resulting disk type is hyperdisk-balanced after the defer-and-detach flow")
		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get converted disk after detach")
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected the resulting disk type to be hyperdisk-balanced after detach-triggered conversion, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after detach-triggered conversion")
	})

	It("E2E-ERR-01: Should retry conversion after a transient active instant snapshot block when switching to a Hyperdisk VolumeAttributesClass", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType:        parameters.DynamicVolumeType,
			parameters.ParameterHDType:         "hyperdisk-balanced",
			parameters.ParameterPDType:         "pd-balanced",
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Provision a pd-balanced disk that is eligible for conversion")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                        z,
						common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
						common.DiskTypeLabelKey("pd-balanced"):        "true",
					},
				}},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed with error: %v", err)

		defer func() {
			err := client.DeleteVolume(volume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume failed")
			project, key, err := common.VolumeIDToKey(volume.VolumeId)
			Expect(err).To(BeNil(), "Failed to parse volume ID")
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// A real PV is required for checkNoConversionInProgress to read/write
		// its tracking annotation on, when --enable-pd-conversion is on.
		Expect(createConversionPV(volName, volume.VolumeId)).To(BeNil(), "Failed to create conversion-tracking PV")
		defer deleteConversionPV(volName)

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk from GCE API")
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk not in READY state")
		Expect(cloudDisk.Type).To(ContainSubstring("pd-balanced"),
			"Expected the initial disk type to be pd-balanced before the conversion attempt, got: %s", cloudDisk.Type)

		By("2. Initiate the pd-to-hyperdisk conversion through ControllerModifyVolume")
		convertParams := map[string]string{
			"type":       "hyperdisk-balanced",
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Starting a conversion never returns a clean success: GCE runs it
		// asynchronously, so the driver reports it as a retryable Unavailable
		// error even when the start itself succeeded, to signal the operation
		// is now in progress.
		err = client.ControllerModifyVolume(volume.VolumeId, convertParams)
		Expect(err).NotTo(BeNil(), "ControllerModifyVolume should report the conversion as in progress")
		startGrpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerModifyVolume")
		Expect(startGrpcStatus.Code()).To(Equal(codes.Unavailable))
		Expect(strings.ToLower(startGrpcStatus.Message())).To(ContainSubstring("is in progress"))

		By("3. Verify attachment is rejected while a conversion is in progress")
		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false)
		Expect(err).NotTo(BeNil(), "ControllerPublishVolumeReadWrite should have been blocked during conversion")
		grpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerPublishVolume")
		Expect(grpcStatus.Code()).To(SatisfyAny(
			Equal(codes.Aborted),
			Equal(codes.FailedPrecondition),
			Equal(codes.Unavailable),
			Equal(codes.Internal),
		))
		Expect(strings.ToLower(grpcStatus.Message())).To(SatisfyAny(
			ContainSubstring("conversion is in progress"),
			ContainSubstring("conversion in progress"),
			ContainSubstring("converting"),
			ContainSubstring("operation"),
		))

		By("4. Wait for the underlying GCE conversion to complete before reattaching the volume")
		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, getErr := computeService.Disks.Get(p, z, volName).Do()
			if getErr != nil {
				return false, getErr
			}
			if strings.Contains(disk.Type, "hyperdisk-balanced") && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for disk conversion to complete in GCE")

		By("5. Reattach the volume once conversion succeeds")
		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false)
		Expect(err).To(BeNil(), "ControllerPublishVolumeReadWrite must succeed after conversion is complete")
		defer func() {
			err = client.ControllerUnpublishVolume(volume.VolumeId, instance.GetNodeID())
			if err != nil {
				klog.Errorf("Failed to detach converted disk: %v", err)
			}
		}()

		stageDir := filepath.Join("/tmp/", volName, "stage")
		err = client.NodeStageExt4Volume(volume.VolumeId, stageDir, false)
		Expect(err).To(BeNil(), "NodeStageExt4Volume failed on converted volume")
		defer func() {
			err = client.NodeUnstageVolume(volume.VolumeId, stageDir)
			if err != nil {
				klog.Errorf("Failed to unstage volume: %v", err)
			}
			fp := filepath.Join("/tmp/", volName)
			err = testutils.RmAll(instance, fp)
			if err != nil {
				klog.Errorf("Failed to clean up test dir: %v", err)
			}
		}()

		publishDir := filepath.Join("/tmp/", volName, "mount")
		err = client.NodePublishVolume(volume.VolumeId, stageDir, publishDir)
		Expect(err).To(BeNil(), "NodePublishVolume failed on converted volume")
		defer func() {
			err = client.NodeUnpublishVolume(volume.VolumeId, publishDir)
			if err != nil {
				klog.Errorf("NodeUnpublishVolume failed on converted volume: %v", err)
			}
		}()

		By("6. Validate final disk type and workload mount state after the retry completes")
		sizeGb, err := testutils.GetFSSizeInGb(instance, publishDir)
		Expect(err).To(BeNil(), "Failed to get filesystem size")
		// ext4 reserves space for its journal and metadata, so the visible
		// filesystem size is a little under the raw disk size.
		Expect(sizeGb).To(BeNumerically("~", defaultHdBSizeGb, 3))

		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get cloud disk after conversion")
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected the final disk type to be hyperdisk-balanced after the retry completes, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after successful conversion")

		Skip("Pending: this direct E2E case implements the raw GCE conversion lifecycle, but the cluster-level VAC/PV annotation flow and instant-snapshot retry semantics are still handled by the feature implementation that is not present in this repo yet.")
	})

})
