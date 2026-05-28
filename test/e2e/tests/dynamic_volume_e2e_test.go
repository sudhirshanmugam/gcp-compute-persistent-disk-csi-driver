/*
Copyright 2024 The Kubernetes Authors.

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
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// waitForSnapshotReady polls until the GCP snapshot reaches READY status.
func waitForSnapshotReady(project, snapshotName string) error {
	return wait.Poll(10*time.Second, 5*time.Minute, func() (bool, error) {
		snap, err := computeService.Snapshots.Get(project, snapshotName).Do()
		if err != nil {
			return false, err
		}
		return snap.Status == "READY", nil
	})
}

var _ = Describe("GCE PD CSI Driver Dynamic Volumes Snapshot of HD Volume", func() {

	It("Should create a snapshot of a hyperdisk-balanced dynamic volume successfully", func() {
		testContext := getRandomMwTestContext()

		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client

		volName := testNamePrefix + string(uuid.NewUUID())

		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  "hyperdisk-balanced",
			parameters.ParameterPDType:  "pd-balanced",
		}

		// Create dynamic volume — should resolve to hyperdisk-balanced on HD node.
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.gke.io/zone": z}},
				},
				Preferred: []*csi.Topology{
					{
						Segments: map[string]string{
							"topology.gke.io/zone":                        z,
							common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
							common.DiskTypeLabelKey("pd-balanced"):        "true",
						},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed: %v", err)
		volID := volume.VolumeId

		defer func() {
			err := client.DeleteVolume(volID)
			Expect(err).To(BeNil(), "DeleteVolume failed")
			_, err = computeService.Disks.Get(p, z, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		// Verify source disk is hyperdisk-balanced.
		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk from GCE API")
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk not in READY state")
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected hyperdisk-balanced source disk but got: %s", cloudDisk.Type)

		// Take a snapshot of the dynamic volume.
		snapshotName := testNamePrefix + string(uuid.NewUUID())
		snapshotID, err := client.CreateSnapshot(snapshotName, volID, nil)
		Expect(err).To(BeNil(), "CreateSnapshot failed: %v", err)

		defer func() {
			err := client.DeleteSnapshot(snapshotID)
			Expect(err).To(BeNil(), "DeleteSnapshot failed")
			_, err = computeService.Snapshots.Get(p, snapshotName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected snapshot to be deleted")
		}()

		// Verify snapshot exists and reaches READY state.
		snapshot, err := computeService.Snapshots.Get(p, snapshotName).Do()
		Expect(err).To(BeNil(), "Could not get snapshot from GCE API")
		Expect(snapshot.Name).To(Equal(snapshotName))

		err = waitForSnapshotReady(p, snapshotName)
		Expect(err).To(BeNil(), "Snapshot did not reach READY state: %v", err)

		// Verify the snapshot references the correct source disk.
		snapshot, err = computeService.Snapshots.Get(p, snapshotName).Do()
		Expect(err).To(BeNil(), "Could not re-fetch snapshot")
		Expect(snapshot.SourceDiskId).NotTo(BeEmpty(), "Snapshot should reference a source disk")

		klog.Infof("Snapshot created: name=%s status=%s sourceDisk=%s",
			snapshot.Name, snapshot.Status, snapshot.SourceDisk)
		Expect(snapshot.SourceDisk).To(ContainSubstring(volName),
			"Snapshot source disk should reference the dynamic volume")
	})

})
