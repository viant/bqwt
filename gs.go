package bqwt

import (
	"cloud.google.com/go/storage"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os"
)

//DownloadGSContent returns google storage content
func DownloadGSContent(ctx context.Context, URL string) ([]byte, error) {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}
	if parsedURL.Scheme == "file" {
		content, err := ioutil.ReadFile(parsedURL.Path)
		if err != nil {
			return nil, reclassifyNotFoundIfMatched(err, URL)
		}
		return content, err
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bucket := client.Bucket(parsedURL.Host)
	objectPath := string(parsedURL.Path[1:])
	rc, err := bucket.Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, reclassifyNotFoundIfMatched(err, URL)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

//UploadGSContent uploads content to gs
func UploadGSContent(ctx context.Context, URL string, reader io.Reader) error {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return err
	}
	if parsedURL.Scheme == "file" {
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		return ioutil.WriteFile(parsedURL.Path, data, 0777)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bucket := client.Bucket(parsedURL.Host)
	objectPath := string(parsedURL.Path[1:])
	writer := bucket.Object(objectPath).NewWriter(ctx)
	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}
	return writer.Close()
}

//ExistsGSObject returns true if  gs object exists
func ExistsGSObject(ctx context.Context, URL string) bool {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return false
	}
	if parsedURL.Scheme == "file" {
		_, err := os.Stat(parsedURL.Path)
		return err == nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return false
	}
	bucket := client.Bucket(parsedURL.Host)
	objectPath := string(parsedURL.Path[1:])
	_, err = bucket.Object(objectPath).Attrs(ctx)
	err = reclassifyNotFoundIfMatched(err, URL)
	return !IsNotFoundError(err)
}

//DeleteGSObject delete gs object
func DeleteGSObject(ctx context.Context, URL string) error {
	parsedURL, err := url.Parse(URL)
	if err != nil {
		return err
	}
	if parsedURL.Scheme == "file" {
		err = os.Remove(parsedURL.Path)
		return err
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	bucket := client.Bucket(parsedURL.Host)
	objectPath := string(parsedURL.Path[1:])
	return bucket.Object(objectPath).Delete(ctx)
}
