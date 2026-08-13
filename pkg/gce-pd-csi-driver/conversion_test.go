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

import (
	"context"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	computebeta "google.golang.org/api/compute/v0.beta"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/k8s-cloud-provider/pkg/cloud/meta"

	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/k8sclient"
)

// conversionPV builds the PersistentVolume backing the test disk. The disk name
// and the PV name are the same for dynamically provisioned volumes, which is
// how the driver finds the PV from a volume ID.
func conversionPV(annotations map[string]string, vacName string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
			UID:         "test-uid",
		},
	}
	if vacName != "" {
		pv.Spec.VolumeAttributesClassName = &vacName
	}
	return pv
}

func conversionVAC(vacName string, params map[string]string) *storagev1beta1.VolumeAttributesClass {
	return &storagev1beta1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: vacName},
		DriverName: "pd.csi.storage.gke.io",
		Parameters: params,
	}
}

// withFakeKubeClient installs a fake clientset for the duration of a test and
// returns it so that the resulting objects can be inspected.
func withFakeKubeClient(t *testing.T, objects ...runtime.Object) kubernetes.Interface {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	old := k8sclient.GetClient
	k8sclient.GetClient = func() (kubernetes.Interface, error) { return client, nil }
	t.Cleanup(func() { k8sclient.GetClient = old })
	return client
}

func pvAnnotations(t *testing.T, client kubernetes.Interface) map[string]string {
	t.Helper()
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get PersistentVolume: %v", err)
	}
	return pv.GetAnnotations()
}

func TestConversionMarksAttachedVolumePending(t *testing.T) {
	client := withFakeKubeClient(t, conversionPV(nil, "vac-hyperdisk"))

	disk := &compute.Disk{
		Name: name, Zone: zone, SelfLink: testVolumeID,
		Type: "pd-balanced", SizeGb: 200,
		Users: []string{"instance-1"},
	}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          testVolumeID,
		MutableParameters: map[string]string{"type": "hyperdisk-balanced"},
	})

	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v (err: %v)", got, err)
	}
	if fcp.ConversionTestParams.TypeConversionCalled {
		t.Error("conversion must not start while the disk is attached")
	}
	if got := pvAnnotations(t, client)[ConversionOperationAnnotation]; got != ConversionPending {
		t.Errorf("expected %q annotation to be %q, got %q", ConversionOperationAnnotation, ConversionPending, got)
	}
}

func TestConversionRecordsOperationOnStart(t *testing.T) {
	client := withFakeKubeClient(t, conversionPV(nil, "vac-hyperdisk"))

	disk := &compute.Disk{Name: name, Zone: zone, SelfLink: testVolumeID, Type: "pd-balanced", SizeGb: 200}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          testVolumeID,
		MutableParameters: map[string]string{"type": "hyperdisk-balanced"},
	})

	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("expected Unavailable while converting, got %v (err: %v)", got, err)
	}
	annotations := pvAnnotations(t, client)
	if annotations[ConversionOperationAnnotation] == "" || annotations[ConversionOperationAnnotation] == ConversionPending {
		t.Errorf("expected an operation self link annotation, got %q", annotations[ConversionOperationAnnotation])
	}
	if got := annotations[ConvertedFromAnnotation]; got != "pd-balanced" {
		t.Errorf("expected source type annotation to be pd-balanced, got %q", got)
	}

	events, err := client.CoreV1().Events(metav1.NamespaceDefault).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list events: %v", err)
	}
	found := false
	for _, event := range events.Items {
		if event.Reason == "DiskTypeConversionStart" {
			found = true
		}
	}
	if !found {
		t.Error("expected a DiskTypeConversionStart event")
	}
}

func TestConversionCompletesWhenDiskReachesTargetType(t *testing.T) {
	client := withFakeKubeClient(t, conversionPV(map[string]string{
		ConversionOperationAnnotation: "https://compute/operations/op-1",
		ConvertedFromAnnotation:       "pd-balanced",
	}, "vac-hyperdisk"))

	disk := &compute.Disk{Name: name, Zone: zone, SelfLink: testVolumeID, Type: "hyperdisk-balanced", SizeGb: 200}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          testVolumeID,
		MutableParameters: map[string]string{"type": "hyperdisk-balanced"},
	})
	if err != nil {
		t.Fatalf("expected the modification to succeed, got %v", err)
	}

	annotations := pvAnnotations(t, client)
	if got, ok := annotations[ConversionOperationAnnotation]; ok && got != "" {
		t.Errorf("expected the operation annotation to be cleared, got %q", got)
	}
	if got := annotations[ConvertedFromAnnotation]; got != "pd-balanced" {
		t.Errorf("expected converted-from to be pd-balanced, got %q", got)
	}
	if got := annotations[ConvertedToAnnotation]; got != "hyperdisk-balanced" {
		t.Errorf("expected converted-to to be hyperdisk-balanced, got %q", got)
	}
}

func TestConversionRejectsMultiWriterDisk(t *testing.T) {
	withFakeKubeClient(t, conversionPV(nil, "vac-hyperdisk"))

	// MultiWriter is only represented on the beta disk resource.
	disk := &computebeta.Disk{
		Name: name, Zone: zone, SelfLink: testVolumeID,
		Type: "pd-balanced", SizeGb: 200, MultiWriter: true,
	}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromBeta(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{
		VolumeId:          testVolumeID,
		MutableParameters: map[string]string{"type": "hyperdisk-balanced"},
	})

	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for a multi-writer disk, got %v (err: %v)", got, err)
	}
	if fcp.ConversionTestParams.TypeConversionCalled {
		t.Error("conversion must not be attempted for a multi-writer disk")
	}
}

func TestAttachBlockedWhileConversionRuns(t *testing.T) {
	withFakeKubeClient(t, conversionPV(map[string]string{
		ConversionOperationAnnotation: "https://compute/operations/op-1",
	}, "vac-hyperdisk"))

	disk := &compute.Disk{Name: name, Zone: zone, SelfLink: testVolumeID, Type: "pd-balanced", SizeGb: 200}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	fcp.ConversionTestParams.OperationStatus = "RUNNING"
	fcp.InsertInstance(&compute.Instance{Name: node, SelfLink: testNodeID}, zone, node)
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         testVolumeID,
		NodeId:           testNodeID,
		VolumeCapability: stdVolCap,
	})

	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition while converting, got %v (err: %v)", got, err)
	}
}

func TestAttachCompletesConversionBookkeeping(t *testing.T) {
	client := withFakeKubeClient(t, conversionPV(map[string]string{
		ConversionOperationAnnotation: "https://compute/operations/op-1",
		ConvertedFromAnnotation:       "pd-balanced",
	}, "vac-hyperdisk"))

	disk := &compute.Disk{Name: name, Zone: zone, SelfLink: testVolumeID, Type: "hyperdisk-balanced", SizeGb: 200}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	fcp.ConversionTestParams.OperationStatus = operationStatusDone
	fcp.InsertInstance(&compute.Instance{Name: node, SelfLink: testNodeID}, zone, node)
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         testVolumeID,
		NodeId:           testNodeID,
		VolumeCapability: stdVolCap,
	})
	if err != nil {
		t.Fatalf("expected the attach to succeed, got %v", err)
	}

	annotations := pvAnnotations(t, client)
	if got, ok := annotations[ConversionOperationAnnotation]; ok && got != "" {
		t.Errorf("expected the operation annotation to be cleared, got %q", got)
	}
	if got := annotations[ConvertedToAnnotation]; got != "hyperdisk-balanced" {
		t.Errorf("expected converted-to to be hyperdisk-balanced, got %q", got)
	}
}

func TestDetachStartsPendingConversion(t *testing.T) {
	client := withFakeKubeClient(t,
		conversionPV(map[string]string{ConversionOperationAnnotation: ConversionPending}, "vac-hyperdisk"),
		conversionVAC("vac-hyperdisk", map[string]string{"type": "hyperdisk-balanced"}),
	)

	disk := &compute.Disk{Name: name, Zone: zone, SelfLink: testVolumeID, Type: "pd-balanced", SizeGb: 200}
	fcp, err := gce.CreateFakeCloudProvider(project, zone, []*gce.CloudDisk{gce.CloudDiskFromV1(disk)})
	if err != nil {
		t.Fatalf("failed to create fake cloud provider: %v", err)
	}
	deviceName, err := common.GetDeviceName(meta.ZonalKey(name, zone))
	if err != nil {
		t.Fatalf("failed to get device name: %v", err)
	}
	instance := &compute.Instance{
		Name:     node,
		SelfLink: testNodeID,
		Disks:    []*compute.AttachedDisk{{DeviceName: deviceName, Source: testVolumeID}},
	}
	fcp.InsertInstance(instance, zone, node)
	gceDriver := initGCEDriverWithCloudProvider(t, fcp, &GCEControllerServerArgs{EnablePdConversion: true})

	_, err = gceDriver.cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: testVolumeID,
		NodeId:   testNodeID,
	})
	if err != nil {
		t.Fatalf("expected the detach to succeed, got %v", err)
	}

	if !fcp.ConversionTestParams.TypeConversionCalled {
		t.Fatal("expected the pending conversion to start after detach")
	}
	if got := fcp.ConversionTestParams.TypeConversionTargetType; got != "hyperdisk-balanced" {
		t.Errorf("expected conversion to hyperdisk-balanced, got %q", got)
	}
	if got := pvAnnotations(t, client)[ConversionOperationAnnotation]; got == ConversionPending || got == "" {
		t.Errorf("expected the operation self link to replace the pending marker, got %q", got)
	}
}
