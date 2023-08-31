// Package renamefile provides a Cloud Function for renaming files
// (trimming prefixes) stored in Google Storage Bucket.
package renamefile

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
	"github.com/jf-tech/go-corelib/ios"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	// Define which file extensions should be processed
	extensions = [2]string{".csv", ".txt"}
	// Global API clients used across function invocations.
	storageClient *storage.Client
	bgctx         = context.Background()
)

func init() {
	// Declare a separate err variable to avoid shadowing the client variables.
	var err error

	// Initialize Storage client
	storageClient, err = storage.NewClient(bgctx)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}

	functions.CloudEvent("ProcessFile", processFile)
}

// processFile moves an object into another location.
func processFile(ctx context.Context, e event.Event) error {
	var metadata storagedata.StorageObjectData
	if err := protojson.Unmarshal(e.Data(), &metadata); err != nil {
		return fmt.Errorf("protojson.Unmarshal: %w", err)
	}

	log.Printf("Bucket: %s", metadata.GetBucket())
	log.Printf("Object: %s", metadata.GetName())

	bucketName := metadata.GetBucket()
	objectName := metadata.GetName()

	for _, ext := range extensions {
		if strings.HasSuffix(objectName, ext) && strings.Contains(objectName, "|") {
			dstObjectName := setDestFileName(objectName, ext)
			saveObject(bucketName, objectName, dstObjectName)
		}
	}

	return nil
}

// setDestFileName performs operations under existing GCS Object name to
// define new, destination Object name
func setDestFileName(srcObjectName, extension string) string {
	fileName := regexp.MustCompile(`.*\/`).ReplaceAllString(srcObjectName, "")
	log.Printf("Filename without path is %s \n", fileName)

	// // Define which exactly prefixes should be replaced
	// var prefixes = [2]string{"66000_", "69000_"}

	// // Removing leading prefixes from file name
	// for _, prefix := range prefixes {
	// 	if strings.HasPrefix(fileName, prefix) {
	// 		dstName = strings.TrimPrefix(fileName, prefix)
	// 	} else {
	// 		dstName = fileName
	// 	}
	// }

	// Cut meaningful part of object name (before separator)
	dstName, _, _ := strings.Cut(fileName, "|")

	// Add extension to new object name if needed
	if !strings.HasSuffix(dstName, extension) {
		dstName = fmt.Sprintf("%s%s", dstName, extension)
	}

	dstObjectName := strings.Replace(srcObjectName, fileName, dstName, 1)

	return dstObjectName
}

// saveObject saves processed object with new name into GCS bucket
// and deletes original object from GCS bucket
func saveObject(bucketName, srcObjectName, dstObjectName string) error {
	bgctx, cancel := context.WithTimeout(bgctx, time.Second*50)
	defer cancel()

	content, _ := replaceQuotes(bucketName, srcObjectName)
	buf := bytes.NewBuffer(content)

	src := storageClient.Bucket(bucketName).Object(srcObjectName)

	// Upload an object with storage.Writer.
	wc := storageClient.Bucket(bucketName).Object(dstObjectName).NewWriter(bgctx)
	wc.ContentType = "application/octet-stream"
	wc.ChunkSize = 0 // note retries are not supported for chunk size 0.

	if _, err := io.Copy(wc, buf); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	// Data can continue to be added to the file until the writer is closed.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %w", err)
	}

	// Delete original object from bucket
	if err := src.Delete(bgctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %w", srcObjectName, err)
	}

	log.Printf("Blob %v moved to %v.\n", srcObjectName, dstObjectName)

	return nil
}

// replaceQuotes recurcively replaces two double quotes in a row into one double qoute, like:
// cat _test_file_20230818.csv' | sed "s/~~/,/g" | sed "s/\"\",\"\"/\",\"/g"
func replaceQuotes(bucketName, objectName string) ([]byte, error) {
	var r io.Reader

	bgctx, cancel := context.WithTimeout(bgctx, time.Second*50)
	defer cancel()

	rc, err := storageClient.Bucket(bucketName).Object(objectName).NewReader(bgctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %w", objectName, err)
	}
	defer rc.Close()

	r = ios.NewBytesReplacingReader(rc, []byte(`~~`), []byte(`,`))
	r = ios.NewBytesReplacingReader(r, []byte(`"",""`), []byte(`","`))

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %w", err)
	}

	return data, nil
}
