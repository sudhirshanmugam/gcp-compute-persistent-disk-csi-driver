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
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// No Matching Node Labels
//
// When Preferred topology has no disk-type.gke.io/* labels (only zone),
// selectDiskTypeFromTopologies() finds no HD or PD match and falls back
// to the built-in default disk type (hyperdisk-balanced).
var _ = Describe("GCE PD CSI Driver Dynamic Volumes No Matching Node Labels", func() {

	It("Should fall back to default disk type when Preferred topology has no disk-type labels", func() {
		testContext := getRandomMwTestContext()

		p, z, _ := testContext.Instance.GetIdentity()
		client := testContext.Client

		volName := testNamePrefix + string(uuid.NewUUID())

		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  "hyperdisk-balanced",
			parameters.ParameterPDType:  "pd-balanced",
		}

		// Preferred topology has only a zone label — no disk-type.gke.io/* labels.
		// Driver falls back to dts.Default (hyperdisk-balanced).
		volume, err := client.CreateVolume(volName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.gke.io/zone": z}},
				},
				Preferred: []*csi.Topology{
					{Segments: map[string]string{"topology.gke.io/zone": z}},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume failed: %v", err)
		volID := volume.VolumeId

		defer func() {
			err := client.DeleteVolume(volID)
			Expect(err).To(BeNil(), "DeleteVolume failed")
			project, key, err := common.VolumeIDToKey(volID)
			Expect(err).To(BeNil(), "Failed to parse volume ID")
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk to be deleted")
		}()

		cloudDisk, err := computeService.Disks.Get(p, z, volName).Do()
		Expect(err).To(BeNil(), "Could not get disk from GCE API")
		Expect(cloudDisk.Status).To(Equal(readyState), "Disk not in READY state")

		klog.Infof("No-label fallback resolved to disk type: %s", cloudDisk.Type)
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected default hyperdisk-balanced fallback but got: %s", cloudDisk.Type)
	})

})

// Mixed Node Pool Disk Selection
//
// In a cluster with both HD-capable and PD-only nodes, verifies that the driver
// selects the correct disk type based on the topology of the node the pod is scheduled on.
var _ = Describe("GCE PD CSI Driver Dynamic Volumes Mixed Node Pool Disk Selection", func() {

	It("Should select hyperdisk-balanced on HD node and pd-balanced on PD node in a mixed pool", func() {
		hdContext := getRandomMwTestContext()
		pdContext := getRandomTestContext()

		hdProject, hdZone, _ := hdContext.Instance.GetIdentity()
		pdProject, pdZone, _ := pdContext.Instance.GetIdentity()

		params := map[string]string{
			parameters.ParameterKeyType: parameters.DynamicVolumeType,
			parameters.ParameterHDType:  "hyperdisk-balanced",
			parameters.ParameterPDType:  "pd-balanced",
		}

		// --- HD node: pod scheduled on c3-standard-4 → expect hyperdisk-balanced ---
		hdVolName := testNamePrefix + string(uuid.NewUUID())
		hdVolume, err := hdContext.Client.CreateVolume(hdVolName, params, defaultHdBSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.gke.io/zone": hdZone}},
				},
				Preferred: []*csi.Topology{
					{
						Segments: map[string]string{
							"topology.gke.io/zone":                        hdZone,
							common.DiskTypeLabelKey("hyperdisk-balanced"): "true",
							common.DiskTypeLabelKey("pd-balanced"):        "true",
						},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume (HD node) failed: %v", err)
		defer func() {
			err := hdContext.Client.DeleteVolume(hdVolume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume (HD) failed")
			project, key, err := common.VolumeIDToKey(hdVolume.VolumeId)
			Expect(err).To(BeNil())
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected HD disk to be deleted")
		}()

		hdDisk, err := computeService.Disks.Get(hdProject, hdZone, hdVolName).Do()
		Expect(err).To(BeNil(), "Could not get HD disk from GCE API")
		Expect(hdDisk.Status).To(Equal(readyState))
		klog.Infof("Mixed pool — HD node resolved to: %s", hdDisk.Type)
		Expect(hdDisk.Type).To(ContainSubstring("hyperdisk-balanced"),
			"Expected hyperdisk-balanced on HD node but got: %s", hdDisk.Type)

		// --- PD node: pod scheduled on n2d-standard-4 → expect pd-balanced ---
		pdVolName := testNamePrefix + string(uuid.NewUUID())
		pdVolume, err := pdContext.Client.CreateVolume(pdVolName, params, defaultSizeGb,
			&csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{Segments: map[string]string{"topology.gke.io/zone": pdZone}},
				},
				Preferred: []*csi.Topology{
					{
						Segments: map[string]string{
							"topology.gke.io/zone":                 pdZone,
							common.DiskTypeLabelKey("pd-balanced"): "true",
							common.DiskTypeLabelKey("pd-standard"): "true",
							common.DiskTypeLabelKey("pd-ssd"):      "true",
							common.DiskTypeLabelKey("pd-extreme"):  "true",
						},
					},
				},
			}, nil)
		Expect(err).To(BeNil(), "CreateVolume (PD node) failed: %v", err)
		defer func() {
			err := pdContext.Client.DeleteVolume(pdVolume.VolumeId)
			Expect(err).To(BeNil(), "DeleteVolume (PD) failed")
			project, key, err := common.VolumeIDToKey(pdVolume.VolumeId)
			Expect(err).To(BeNil())
			_, err = computeService.Disks.Get(project, key.Zone, key.Name).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected PD disk to be deleted")
		}()

		pdDisk, err := computeService.Disks.Get(pdProject, pdZone, pdVolName).Do()
		Expect(err).To(BeNil(), "Could not get PD disk from GCE API")
		Expect(pdDisk.Status).To(Equal(readyState))
		klog.Infof("Mixed pool — PD node resolved to: %s", pdDisk.Type)
		Expect(pdDisk.Type).To(ContainSubstring("pd-balanced"),
			"Expected pd-balanced on PD node but got: %s", pdDisk.Type)
	})

})
