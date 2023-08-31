// Package exporttonas provides a Cloud Function for exporting files
// from Google Storage Bucket to NAS (smb).
package exporttonas

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"path"
	"time"

	"github.com/hirochachacha/go-smb2"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	NAS_HOST  = "host"
	NAS_USER  = "share"
	NAS_SHARE = "share"
)

// Global API clients used across function invocations.
var (
	storageClient *storage.Client
	bgctx         = context.Background()
	NAS_PASS      = ""
)

type SMBClient struct {
	conn    net.Conn
	dialer  *smb2.Dialer
	session *smb2.Session
	share   *smb2.Share
}

func init() {
	// Declare a separate err variable to avoid shadowing the client variables.
	var err error

	NAS_PASS, err = accessSecretVersion("projects/my-project/secrets/nas-pass/versions/latest")
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	// Initialize Storage client
	storageClient, err = storage.NewClient(bgctx)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
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

	nasClient, err := newSMBClient(NAS_HOST, NAS_USER, NAS_PASS, NAS_SHARE)
	if err != nil {
		panic(err)
	}
	defer nasClient.close()

	return nasClient.upload(objectName, data)
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

func newSMBClient(server, username, password, sharename string) (*SMBClient, error) {
	conn, err := net.Dial("tcp", server+":445")
	if err != nil {
		return nil, err
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     username,
			Password: password,
		},
	}

	s, err := d.Dial(conn)
	if err != nil {
		return nil, err
	}

	share, err := s.Mount(sharename)
	if err != nil {
		return nil, err
	}

	return &SMBClient{
		conn:    conn,
		dialer:  d,
		session: s,
		share:   share,
	}, nil
}

func (c *SMBClient) close() {
	c.share.Umount()
	c.session.Logoff()
	c.conn.Close()
}

func (c *SMBClient) upload(filename string, data []byte) error {
	folder := path.Dir(filename)
	if folder != "" {
		in, err := c.share.Stat(folder)
		if err != nil || !in.IsDir() {
			err := c.share.MkdirAll(folder, 0755)
			if err != nil {
				return err
			}
		}
	}

	dstFile, err := c.share.Create(filename)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("unable to upload file: %v", err)
	}
	log.Printf("%d bytes copied\n", bytes)

	return nil
}
