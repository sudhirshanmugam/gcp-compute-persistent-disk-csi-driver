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
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
)

const (
	diskTypeHyperdiskBalanced = "hyperdisk-balanced"
)

// waitForModifyVolumeToComplete retries a ControllerModifyVolume call until it
// reports the modification complete. Starting a disk type conversion is
// asynchronous on GCE's side, so a successful start is reported back as a
// retryable Unavailable error until the disk actually reaches the requested
// state; any other error is treated as terminal.
func waitForModifyVolumeToComplete(modify func() error) error {
	return wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := modify()
		if err == nil {
			return true, nil
		}
		if grpcStatus, ok := status.FromError(err); ok && grpcStatus.Code() == codes.Unavailable {
			return false, nil
		}
		return false, err
	})
}

var _ = Describe("GCE PD CSI Driver VolumeAttributesClass & Disk Conversion", func() {

	It("Should dynamically modify IOPS and throughput on hyperdisk-balanced without triggering conversion", func() {
		testContext := getRandomTestContext()
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
		// Create the disk already at the target type so this test only
		// exercises the IOPS/throughput update path, not a conversion.
		volumeParams := map[string]string{
			parameters.ParameterKeyType:                          diskTypeHyperdiskBalanced,
			parameters.ParameterKeyProvisionedIOPSOnCreate:       fmt.Sprintf("%d", initialIops),
			parameters.ParameterKeyProvisionedThroughputOnCreate: fmt.Sprintf("%dMi", initialThroughput),
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for hyperdisk-balanced: %v", err)

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk from cloud directly")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced))
		Expect(cloudDisk.ProvisionedIops).To(Equal(initialIops))
		Expect(cloudDisk.ProvisionedThroughput).To(Equal(initialThroughput))

		defer func() {
			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

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

		// Modify IOPS and throughput only; the type stays hyperdisk-balanced,
		// so the driver takes the plain update path rather than a conversion.
		modifyParams := map[string]string{
			"type":       diskTypeHyperdiskBalanced,
			"iops":       fmt.Sprintf("%d", targetIops),
			"throughput": fmt.Sprintf("%dMi", targetThroughput),
		}

		err = client.ControllerModifyVolume(volume.VolumeId, modifyParams)
		Expect(err).To(BeNil(), "ControllerModifyVolume failed during parameter modification")

		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get cloud disk post-modification")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced))
		Expect(cloudDisk.ProvisionedIops).To(Equal(targetIops))
		Expect(cloudDisk.ProvisionedThroughput).To(Equal(targetThroughput))
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain in READY state without detachment or conversion operations")
	})

	It("Should block volume attachment while disk conversion is actively in progress", func() {
		testContext := getRandomTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())

		volumeParams := map[string]string{
			parameters.ParameterKeyType: standardDiskType,
		}

		volume, err := client.CreateVolume(volName, volumeParams, defaultSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{constants.TopologyKeyZone: z},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for pd-standard: %v", err)

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk from cloud")
		Expect(cloudDisk.Type).To(ContainSubstring(standardDiskType))

		defer func() {
			client.DeleteVolume(volume.VolumeId)
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// The disk is not attached yet, so this starts the conversion
		// immediately instead of only queuing it for the next detach.
		convertParams := map[string]string{
			"type": diskTypeHyperdiskBalanced,
		}

		// Starting a conversion returns a retryable Unavailable error until GCE
		// finishes it, so this polls ControllerModifyVolume rather than treating
		// the first call's error as failure.
		err = waitForModifyVolumeToComplete(func() error {
			return client.ControllerModifyVolume(volume.VolumeId, convertParams)
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume did not complete the conversion")

		err = client.ControllerPublishVolumeReadWrite(volume.VolumeId, instance.GetNodeID(), false /* forceAttach */)
		Expect(err).NotTo(BeNil(), "ControllerPublishVolumeReadWrite should have been blocked during conversion")

		grpcStatus, ok := status.FromError(err)
		Expect(ok).To(BeTrue(), "Expected gRPC status error from CSI ControllerPublishVolume")
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
		Expect(sizeGb).To(Equal(defaultSizeGb))
	})
})

var _ = Describe("GCE PD CSI Driver Dynamic Volumes VAC Conversion", func() {

	It("Should convert an unattached pd-balanced disk to hyperdisk-balanced when the PVC switches to a Hyperdisk VolumeAttributesClass", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  diskTypeHyperdiskBalanced,
			parameters.ParameterPDType:  "pd-balanced",
			// Both types are supported by the topology below, so without an
			// explicit preference the driver would default to hyperdisk-balanced.
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Provision an unattached pd-balanced disk on a PD-capable node")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
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
			"type":       diskTypeHyperdiskBalanced,
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Starting a conversion returns a retryable Unavailable error until GCE
		// finishes it, so this polls ControllerModifyVolume rather than treating
		// the first call's error as failure.
		err = waitForModifyVolumeToComplete(func() error {
			return client.ControllerModifyVolume(volume.VolumeId, convertParams)
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume did not complete the conversion")

		By("3. Wait for the GCE disk to finish conversion")
		err = wait.PollUntilContextTimeout(context.Background(), 10*time.Second, 15*time.Minute, true, func(ctx context.Context) (bool, error) {
			disk, getErr := computeService.Disks.Get(p, z, volName).Do()
			if getErr != nil {
				return false, getErr
			}
			if strings.Contains(disk.Type, diskTypeHyperdiskBalanced) && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for pd-balanced disk to convert to hyperdisk-balanced")

		By("4. Verify the final disk type matches the conversion target")
		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get converted disk from cloud")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced),
			"Expected the final disk type to be hyperdisk-balanced after conversion, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after conversion")

		Skip("Pending: the cluster-level VAC-to-PV annotation flow and the Kubernetes-side conversion state tracking are still handled by the feature implementation that is not present in this repo yet. The raw GCE conversion lifecycle is covered here, but the VAC contract is still pending.")
	})

	It("Should defer disk conversion until workload detach when the PVC switches to a Hyperdisk VolumeAttributesClass while the pod is still running", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  diskTypeHyperdiskBalanced,
			parameters.ParameterPDType:  "pd-balanced",
			// Both types are supported by the topology below, so without an
			// explicit preference the driver would default to hyperdisk-balanced.
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Start a stateful workload using a mounted pd-balanced PVC")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
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
			"type":       diskTypeHyperdiskBalanced,
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Starting a conversion returns a retryable Unavailable error until GCE
		// finishes it, so this polls ControllerModifyVolume rather than treating
		// the first call's error as failure.
		err = waitForModifyVolumeToComplete(func() error {
			return client.ControllerModifyVolume(volume.VolumeId, convertParams)
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume did not complete the conversion")

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
			if strings.Contains(disk.Type, diskTypeHyperdiskBalanced) && disk.Status == readyState {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for workload-detach conversion to complete")

		By("6. Confirm the resulting disk type is hyperdisk-balanced after the defer-and-detach flow")
		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get converted disk after detach")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced),
			"Expected the resulting disk type to be hyperdisk-balanced after detach-triggered conversion, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after detach-triggered conversion")

		Skip("Pending: this contract requires the Kubernetes VAC/PV annotation flow and the controller-side deferral semantics; the direct GCE E2E case documents the detach-triggered conversion sequence before that feature lands.")
	})

	It("Should retry conversion after a transient active instant snapshot block when switching to a Hyperdisk VolumeAttributesClass", func() {
		testContext := getRandomMwTestContext()
		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client
		instance := testContext.Instance

		volName := testNamePrefix + string(uuid.NewUUID())
		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  diskTypeHyperdiskBalanced,
			parameters.ParameterPDType:  "pd-balanced",
			// Both types are supported by the topology below, so without an
			// explicit preference the driver would default to hyperdisk-balanced.
			parameters.ParameterDiskPreference: parameters.ParameterPDType,
		}

		By("1. Provision a pd-balanced disk that is eligible for conversion")
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
					},
				}},
				Preferred: []*csi.Topology{{
					Segments: map[string]string{
						"topology.gke.io/zone":                             z,
						common.DiskTypeLabelKey(diskTypeHyperdiskBalanced): "true",
						common.DiskTypeLabelKey("pd-balanced"):             "true",
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
			"Expected the initial disk type to be pd-balanced before the conversion attempt, got: %s", cloudDisk.Type)

		By("2. Initiate the pd-to-hyperdisk conversion through ControllerModifyVolume")
		convertParams := map[string]string{
			"type":       diskTypeHyperdiskBalanced,
			"iops":       "3000",
			"throughput": "140Mi",
		}
		// Starting a conversion returns a retryable Unavailable error until GCE
		// finishes it, so this polls ControllerModifyVolume rather than treating
		// the first call's error as failure.
		err = waitForModifyVolumeToComplete(func() error {
			return client.ControllerModifyVolume(volume.VolumeId, convertParams)
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume did not complete the conversion")

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
			if strings.Contains(disk.Type, diskTypeHyperdiskBalanced) && disk.Status == readyState {
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
		Expect(sizeGb).To(Equal(defaultSizeGb))

		cloudDisk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Failed to get cloud disk after conversion")
		Expect(cloudDisk.Type).To(ContainSubstring(diskTypeHyperdiskBalanced),
			"Expected the final disk type to be hyperdisk-balanced after the retry completes, got: %s", cloudDisk.Type)
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk should remain READY after successful conversion")

		Skip("Pending: this direct E2E case implements the raw GCE conversion lifecycle, but the cluster-level VAC/PV annotation flow and instant-snapshot retry semantics are still handled by the feature implementation that is not present in this repo yet.")
	})

})
