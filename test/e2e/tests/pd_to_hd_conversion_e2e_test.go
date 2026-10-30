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
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GCE PD CSI Driver ControllerModifyVolume", func() {
	It("should modify IOPS and throughput on the same disk type without triggering conversion", func() {
		if !runCMVTests() {
			Skip("Not running ControllerModifyVolume tests, as RUN_CONTROLLER_MODIFY_VOLUME_TESTS is falsy")
		}

		testContext := getRandomMwTestContext()
		client := testContext.Client
		p, z, _ := testContext.Instance.GetIdentity()
		volName := testNamePrefix + string(uuid.NewUUID())

		initialIOPS := provisionedIOPSOnCreateHdbInt
		initialThroughput := provisionedThroughputOnCreateHdbInt
		updatedIOPS := int64(3013)
		updatedThroughput := int64(181)

		params := map[string]string{
			parameters.ParameterKeyType:                          hdbDiskType,
			parameters.ParameterKeyProvisionedIOPSOnCreate:       provisionedIOPSOnCreateHdb,
			parameters.ParameterKeyProvisionedThroughputOnCreate: provisionedThroughputOnCreateHdb,
		}
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: z}},
			},
		}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed: %v", err)

		defer func() {
			err := client.DeleteVolume(volume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume failed: %v", err)
			project, key, err := common.VolumeIDToKey(volume.VolumeId)
			Expect(err).To(BeNil(), "Failed to parse volume ID: %v", err)
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		disk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk before modification: %v", err)
		Expect(disk.Status).To(Equal(readyState))
		Expect(disk.Type).To(ContainSubstring(hdbDiskType))
		Expect(disk.ProvisionedIops).To(Equal(initialIOPS))
		Expect(disk.ProvisionedThroughput).To(Equal(initialThroughput))
		initialDiskType := disk.Type

		err = client.ControllerModifyVolume(volume.VolumeId, map[string]string{
			"type":       hdbDiskType,
			"iops":       "3013",
			"throughput": "181Mi",
		})
		Expect(err).To(BeNil(), "ControllerModifyVolume should update performance without starting a conversion: %v", err)

		err = wait.Poll(10*time.Second, 5*time.Minute, func() (bool, error) {
			disk, err := computeService.Disks.Get(p, z, volName).Do()
			if err != nil {
				return false, err
			}
			return disk.ProvisionedIops == updatedIOPS && disk.ProvisionedThroughput == updatedThroughput, nil
		})
		Expect(err).To(BeNil(), "Timed out waiting for IOPS and throughput to update: %v", err)

		disk, err = computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk after modification: %v", err)
		Expect(disk.Type).To(Equal(initialDiskType), "Disk type changed during performance modification")
		Expect(disk.ProvisionedIops).To(Equal(updatedIOPS))
		Expect(disk.ProvisionedThroughput).To(Equal(updatedThroughput))

		klog.Infof("Updated IOPS and throughput on %s without changing disk type %s", volName, disk.Type)
	})
})
