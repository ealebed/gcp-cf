// Package renamefile provides a Cloud Function for renaming files
// (trimming prefixes) stored in Google Storage Bucket.
package renamefile

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
	"google.golang.org/protobuf/encoding/protojson"
)

func init() {
	functions.CloudEvent("RenameFile", renameFile)
}

// renameFile moves an object into another location.
func renameFile(ctx context.Context, e event.Event) error {
	var metadata storagedata.StorageObjectData
	if err := protojson.Unmarshal(e.Data(), &metadata); err != nil {
		return fmt.Errorf("protojson.Unmarshal: %w", err)
	}

	log.Printf("Bucket: %s", metadata.GetBucket())
	log.Printf("File: %s", metadata.GetName())

	bucketName := metadata.GetBucket()
	objectName := metadata.GetName()

	return trimPrefix(bucketName, objectName)
}

// trimPrefix trims prefix and save object with new name
func trimPrefix(bucketName, objectName string) error {
	if strings.HasPrefix(objectName, "0000") {
		bgctx := context.Background()
		client, err := storage.NewClient(bgctx)
		if err != nil {
			return fmt.Errorf("storage.NewClient: %w", err)
		}
		defer client.Close()

		bgctx, cancel := context.WithTimeout(bgctx, time.Second*10)
		defer cancel()

		dstName := strings.TrimPrefix(objectName, "0000")
		src := client.Bucket(bucketName).Object(objectName)
		dst := client.Bucket(bucketName).Object(dstName)

		// Optional: set a generation-match precondition to avoid potential race
		// conditions and data corruptions. The request to copy the file is aborted
		// if the object's generation number does not match your precondition.
		// For a dst object that does not yet exist, set the DoesNotExist precondition.
		dst = dst.If(storage.Conditions{DoesNotExist: true})
		// If the destination object already exists in your bucket, set instead a
		// generation-match precondition using its generation number.
		// attrs, err := dst.Attrs(bgctx)
		// if err != nil {
		//      return fmt.Errorf("object.Attrs: %w", err)
		// }
		// dst = dst.If(storage.Conditions{GenerationMatch: attrs.Generation})

		if _, err := dst.CopierFrom(src).Run(bgctx); err != nil {
			return fmt.Errorf("Object(%q).CopierFrom(%q).Run: %w", dstName, objectName, err)
		}
		if err := src.Delete(bgctx); err != nil {
			return fmt.Errorf("Object(%q).Delete: %w", objectName, err)
		}

		log.Printf("Blob %v moved to %v.\n", objectName, dstName)
	} else {
		log.Printf("Object %v should not be renamed.\n", objectName)
	}

	return nil
}
