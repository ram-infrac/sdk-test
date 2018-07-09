package sanity

import (
	"context"

	"github.com/libopenstorage/openstorage/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Enumerate [OpenStorageCluster]", func() {
	var (
		credClient   api.OpenStorageCredentialsClient
		volClient    api.OpenStorageVolumeClient
		backupClient api.OpenStorageCloudBackupClient
		volID        string
		credID       string
	)

	BeforeEach(func() {

		credClient = api.NewOpenStorageCredentialsClient(conn)
		backupClient = api.NewOpenStorageCloudBackupClient(conn)
		volClient = api.NewOpenStorageVolumeClient(conn)

	})

	AfterEach(func() {
		if volID != "" {
			_, err := volClient.Detach(
				context.Background(),
				&api.SdkVolumeDetachRequest{
					VolumeId: volID,
				},
			)
			Expect(err).NotTo(HaveOccurred())
			_, err = volClient.Delete(
				context.Background(),
				&api.SdkVolumeDeleteRequest{VolumeId: volID},
			)
			Expect(err).NotTo(HaveOccurred())
		}
		if credID != "" {
			_, err := credClient.Delete(
				context.Background(),
				&api.SdkCredentialDeleteRequest{
					CredentialId: credID,
				},
			)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("Should Create backup for provided volume", func() {
		volID = newTestVolume(volClient)
		credID = newTestCredential(credClient)
		//var bkpStatus map[string]*api.SdkCloudBackupStatus
		credsUUIDMap := parseAndCreateCredentials(credClient)
		for provider, uuid := range credsUUIDMap {
			credID = uuid
			By("Doing Backup on " + provider)

			// Attaching the volume first
			str, err := volClient.Attach(
				context.Background(),
				&api.SdkVolumeAttachRequest{
					VolumeId: volID,
				},
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(str).NotTo(BeNil())

			backupReq := &api.SdkCloudBackupCreateRequest{
				VolumeId:       volID,
				CredentialUuid: credID,
				Full:           false,
			}

			backupResp, err := backupClient.Create(context.Background(), backupReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(backupResp.GetBackupId()).NotTo(BeEmpty())
			/*  TODO: status should return volume ID for backup , fake driver returns backupID
			recheck this
						By("Checking backup status")

						// timeout after 5 mins
						timeout := 300
						timespent := 0
						for timespent < timeout {
							statReq := &api.SdkCloudBackupStatusRequest{
								SrcVolumeId: volID,
							}

							statResp, err := backupClient.Status(context.Background(), statReq)
							Expect(err).NotTo(HaveOccurred())
							Expect(statResp.GetStatuses()).NotTo(BeEmpty())

							bkpStatus = statResp.GetStatuses()

							if bkpStatus[volID].Status == api.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeDone {
								break
							}
							if bkpStatus[volID].Status == api.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeActive {
								time.Sleep(time.Second * 10)
								timeout += 10
							}
							if bkpStatus[volID].Status == api.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeFailed {
								break
							}
						}
						Expect(bkpStatus[volID].Status).To(BeEquivalentTo(api.SdkCloudBackupStatusType_SdkCloudBackupStatusTypeDone))
					}
			*/
		}
	})

	It("Should fail to create backup for non-existance volume", func() {
		volID = ""
		credID = newTestCredential(credClient)

		backupReq := &api.SdkCloudBackupCreateRequest{
			VolumeId:       volID,
			CredentialUuid: credID,
			Full:           false,
		}

		backupResp, err := backupClient.Create(context.Background(), backupReq)
		Expect(err).To(HaveOccurred())
		Expect(backupResp.GetBackupId()).To(BeEmpty())

		serverError, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(serverError.Code()).To(BeEquivalentTo(codes.InvalidArgument))

	})

	It("Should fail to create backup for non-existance credential", func() {
		volID = newTestVolume(volClient)
		credID = ""

		backupReq := &api.SdkCloudBackupCreateRequest{
			VolumeId:       volID,
			CredentialUuid: credID,
			Full:           false,
		}

		backupResp, err := backupClient.Create(context.Background(), backupReq)
		Expect(err).To(HaveOccurred())
		Expect(backupResp.GetBackupId()).To(BeEmpty())

		serverError, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(serverError.Code()).To(BeEquivalentTo(codes.InvalidArgument))

	})

	It("Should restore backup for provided backup id", func() {
		volID = newTestVolume(volClient)
		credID = newTestCredential(credClient)
		restoreVol := "restore-vol-1"

		backupReq := &api.SdkCloudBackupCreateRequest{
			VolumeId:       volID,
			CredentialUuid: credID,
			Full:           false,
		}

		backupResp, err := backupClient.Create(context.Background(), backupReq)
		Expect(err).NotTo(HaveOccurred())
		Expect(backupResp.GetBackupId()).NotTo(BeEmpty())

		// TODO: Here backupID is TBD-- what is supposed to be done here?
		restoreReq := &api.SdkCloudBackupRestoreRequest{
			BackupId:          backupResp.GetBackupId(),
			RestoreVolumeName: restoreVol,
			CredentialUuid:    credID,
		}

		restoreResp, err := backupClient.Restore(context.Background(), restoreReq)
		Expect(err).NotTo(HaveOccurred())
		Expect(restoreResp.GetRestoreVolumeId()).NotTo(BeEmpty())
		Expect(restoreResp.GetRestoreVolumeId()).To(BeEquivalentTo(volID))

		inspectResp, err := volClient.Inspect(
			context.Background(),
			&api.SdkVolumeInspectRequest{
				VolumeId: restoreResp.GetRestoreVolumeId(),
			},
		)

		Expect(err).NotTo(HaveOccurred())
		Expect(inspectResp).NotTo(BeNil())

	})

	It("Should failed to restore backup for non-existance backup id", func() {
		volID = newTestVolume(volClient)
		credID = newTestCredential(credClient)

		restoreReq := &api.SdkCloudBackupRestoreRequest{
			BackupId:       "",
			CredentialUuid: credID,
		}

		restoreResp, err := backupClient.Restore(context.Background(), restoreReq)
		Expect(err).To(HaveOccurred())
		Expect(restoreResp.GetRestoreVolumeId()).To(BeEmpty())

	})

})
