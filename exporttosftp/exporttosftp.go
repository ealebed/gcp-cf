// Package exporttosftp provides a Cloud Function for exporting files
// from Google Storage Bucket to SFTP.
package exporttosftp

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
	"golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/pkg/sftp"
)

const (
	SFTP_HOST   = "host"
	SFTP_PORT   = 22
	SFTP_USER   = "user"
	SFTP_FOLDER = "folder"
)

// Global API clients used across function invocations.
var (
	storageClient *storage.Client
	sftpClient    *sftp.Client
	bgctx         = context.Background()
	SFTP_PASS     = ""
)

func init() {
	// Declare a separate err variable to avoid shadowing the client variables.
	var err error

	SFTP_PASS, err = accessSecretVersion("projects/my-project/secrets/secret-name/versions/latest")
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	// Initialize Storage client
	storageClient, err = storage.NewClient(bgctx)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}

	// Initialize SFTP client configuration
	sftpConfig := ssh.ClientConfig{
		User: SFTP_USER,
		// Ignore host key check
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.Password(SFTP_PASS),
		},
	}

	addr := fmt.Sprintf("%s:%d", SFTP_HOST, SFTP_PORT)

	// Connect to server
	sshConn, err := ssh.Dial("tcp", addr, &sftpConfig)
	if err != nil {
		log.Fatalf("failed to connect to [%s]: %v", addr, err)
	}

	// Initialize SFTP client
	sftpClient, err = sftp.NewClient(sshConn)
	if err != nil {
		log.Fatalf("unable to start SFTP subsystem: %v", err)
	}

	functions.CloudEvent("ExportFiles", exportFiles)
}

// exportFiles consumes a CloudEvent message with changed object.
func exportFiles(ctx context.Context, e event.Event) error {
	var metadata storagedata.StorageObjectData
	if err := protojson.Unmarshal(e.Data(), &metadata); err != nil {
		return fmt.Errorf("protojson.Unmarshal: %w", err)
	}

	log.Printf("Bucket: %s", metadata.GetBucket())
	log.Printf("File: %s", metadata.GetName())

	objectName := metadata.GetName()
	bucketName := metadata.GetBucket()

	data, err := downloadFileIntoMemory(bgctx, bucketName, objectName)
	if err != nil {
		return fmt.Errorf("unable download object %s from bucket %s: %v", objectName, bucketName, err)
	}

	return uploadFileToSFTP(objectName, data)
}

// downloadFileIntoMemory downloads an object.
func downloadFileIntoMemory(bgctx context.Context, bucket, object string) ([]byte, error) {
	bgctx, cancel := context.WithTimeout(bgctx, time.Second*50)
	defer cancel()

	rc, err := storageClient.Bucket(bucket).Object(object).NewReader(bgctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %w", err)
	}
	log.Printf("Blob %v downloaded.\n", object)

	return data, nil
}

// uploadFileToSFTP uploads an object.
func uploadFileToSFTP(filename string, data []byte) error {
	// Set the destination for the object
	dstFile := fmt.Sprintf("%s/%s", SFTP_FOLDER, filename)
	log.Printf("Uploading [%s] to [%s] ...\n", filename, dstFile)

	// Note: SFTP To Go doesn't support O_RDWR mode
	destFile, err := sftpClient.OpenFile(dstFile, (os.O_WRONLY | os.O_CREATE | os.O_TRUNC))
	if err != nil {
		return fmt.Errorf("unable to open remote file: %v", err)
	}
	defer destFile.Close()

	bytes, err := io.Copy(destFile, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("unable to upload local file: %v", err)
	}
	log.Printf("%d bytes copied\n", bytes)

	return nil
}

// accessSecretVersion accesses the payload for the given secret version if one
// exists. The version can be a version number as a string (e.g. "5") or an
// alias (e.g. "latest").
func accessSecretVersion(name string) (string, error) {
	// name := "projects/my-project/secrets/my-secret/versions/5"
	// name := "projects/my-project/secrets/my-secret/versions/latest"

	// Create the client.
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create secretmanager client: %w", err)
	}
	defer client.Close()

	// Build the request.
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	}

	// Call the API.
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %w", err)
	}

	// Verify the data checksum.
	crc32c := crc32.MakeTable(crc32.Castagnoli)
	checksum := int64(crc32.Checksum(result.Payload.Data, crc32c))
	if checksum != *result.Payload.DataCrc32C {
		return "", fmt.Errorf("data corruption detected")
	}

	secret := string(result.Payload.Data)

	return secret, nil
}
