// Package exporttosftp provides a Cloud Function for exporting files
// from Google Storage Bucket to SFTP server.
package exporttosftp

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path"
	"strings"
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

var (
	// Define which file extensions should be processed
	extensions = [2]string{".csv", ".txt"}
	// Global API clients used across function invocations
	storageClient *storage.Client
	sftpClient    *sftp.Client
	bgctx         = context.Background()
	// SFTP server related variables
	SFTP_HOST   = ""
	SFTP_PORT   = "22"
	SFTP_USER   = ""
	SFTP_PASS   = ""
	SFTP_FOLDER = ""
)

func init() {
	// Declare a separate err variable to avoid shadowing the client variables
	var err error

	projectID := os.Getenv("_PROJECT_ID")

	// Get SFTP host from GCP Secret Manager
	SFTP_HOST, err = accessSecretVersion("projects/" + projectID + "/secrets/sftp-host/versions/latest")
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	// Get SFTP username from GCP Secret Manager
	SFTP_USER, err = accessSecretVersion("projects/" + projectID + "/secrets/sftp-user/versions/latest")
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	// Get SFTP password from GCP Secret Manager
	SFTP_PASS, err = accessSecretVersion("projects/" + projectID + "/secrets/sftp-pass/versions/latest")
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	// Get SFTP port from environment variable
	if os.Getenv("SFTP_PORT") != "" {
		SFTP_PORT = os.Getenv("SFTP_PORT")
	}

	// Get SFTP folder from environment variable
	if os.Getenv("SFTP_FOLDER") != "" {
		SFTP_FOLDER = os.Getenv("SFTP_FOLDER")
	}

	// Initialize Storage client
	storageClient, err = storage.NewClient(bgctx)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}

	functions.CloudEvent("ExportFiles", exportFiles)
}

// exportFiles consumes a CloudEvent message with changed object
func exportFiles(ctx context.Context, e event.Event) error {
	var metadata storagedata.StorageObjectData
	if err := protojson.Unmarshal(e.Data(), &metadata); err != nil {
		return fmt.Errorf("protojson.Unmarshal: %w", err)
	}

	// Just for debug
	log.Printf("Bucket: %s", metadata.GetBucket())
	log.Printf("File: %s", metadata.GetName())

	objectName := metadata.GetName()
	bucketName := metadata.GetBucket()

	for _, ext := range extensions {
		// Process file only if object name NOT contains '|' and file extension are '.csv' or '.txt'
		if strings.HasSuffix(objectName, ext) && !strings.Contains(objectName, "|") {
			// download an object from GCS buket into memory
			data, err := downloadFileIntoMemory(bgctx, bucketName, objectName)
			if err != nil {
				return fmt.Errorf("unable download object %s from bucket %s: %v", objectName, bucketName, err)
			}

			// Initialize SMB client
			if err := newSFTPClient(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASS); err != nil {
				return fmt.Errorf("io.Copy: %w", err)
			}

			uploadToSFTP(objectName, SFTP_FOLDER, data)
		}
	}

	return nil
}

// downloadFileIntoMemory downloads an object in streaming fashion
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

// newSFTPClient returns pointer to the configured SFTP Client
func newSFTPClient(server, port, username, password string) error {
	// Initialize SFTP client configuration
	sftpConfig := ssh.ClientConfig{
		User: username,
		// Ignore host key check
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
	}

	addr := fmt.Sprintf("%s:%s", server, port)

	// Connect to server
	sshConn, err := ssh.Dial("tcp", addr, &sftpConfig)
	if err != nil {
		log.Fatalf("failed to connect to [%s]: %v", addr, err)
		return err
	}

	// Initialize SFTP client
	sftpClient, err = sftp.NewClient(sshConn)
	if err != nil {
		log.Fatalf("unable to start SFTP subsystem: %v", err)
		return err
	}

	return nil
}

// uploadToSFTP uploads an object to remote SFTP server
func uploadToSFTP(filename, folder string, data []byte) error {
	// Set the destination for the object
	dstFile := fmt.Sprintf("%s/%s", folder, filename)
	log.Printf("Uploading [%s] to [%s] ...\n", filename, dstFile)

	// check path on the remote server and create directories if needed
	dir := path.Dir(dstFile)
	if dir != "" {
		in, err := sftpClient.Stat(dir)
		if err != nil || !in.IsDir() {
			err := sftpClient.MkdirAll(dir)
			if err != nil {
				return err
			}
		}
	}

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
// alias (e.g. "latest")
func accessSecretVersion(name string) (string, error) {
	// name := "projects/my-project/secrets/my-secret/versions/5"
	// name := "projects/my-project/secrets/my-secret/versions/latest"

	// Create the client
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create secretmanager client: %w", err)
	}
	defer client.Close()

	// Build the request
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	}

	// Call the API
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %w", err)
	}

	// Verify the data checksum
	crc32c := crc32.MakeTable(crc32.Castagnoli)
	checksum := int64(crc32.Checksum(result.Payload.Data, crc32c))
	if checksum != *result.Payload.DataCrc32C {
		return "", fmt.Errorf("data corruption detected")
	}

	secret := string(result.Payload.Data)

	return secret, nil
}
