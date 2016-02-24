package backend

import (
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Backend struct {
	bucket string
	path   string
	svc    *s3.S3
}

func NewS3Backend(bucket string, s3path string, sess *session.Session) *S3Backend {
	return &S3Backend{
		bucket: bucket,
		path:   strings.TrimPrefix(path.Clean(s3path), "/"),
		svc:    s3.New(sess),
	}
}

func (s *S3Backend) LatestVersion(checkForSuccess bool) (string, error) {
	// This code assumes you're using S3 like a filesystem, with directories
	// separated by /'s. It also ignores the trailing slash on a prefix (for the
	// purposes of sorting lexicographically), to be consistent with other
	// backends.
	var version, marker string

	for {
		params := &s3.ListObjectsInput{
			Bucket:		aws.String(s.bucket),
			Delimiter:	aws.String("/"),
			Marker:		aws.String(marker),
			MaxKeys:	aws.Int64(1000),
			Prefix:		aws.String(s.path+"/"),
		}
		resp, err := s.svc.ListObjects(params)

		if err != nil {
			return "", s.s3error(err)
		} else if resp.CommonPrefixes == nil {
			break
		}

		var prefix string
		// Search backwards for a valid version
		for i := len(resp.CommonPrefixes) - 1; i >= 0; i-- {
			prefix = strings.TrimSuffix(resp.CommonPrefixes[i].String(), "/")
			if path.Base(prefix) <= version {
				continue
			}

			valid := false
			if checkForSuccess {
				// If the 'directory' has a key _SUCCESS inside it, that implies that
				// there might be other keys with the same prefix
				successFile := path.Join(prefix, "_SUCCESS")
				exists, err := s.exists(successFile)

				if err != nil {
					return "", err
				}

				valid = exists
			} else {
				// Otherwise, we just check the prefix itself. If it doesn't exist, then
				// it's a prefix for some other key, and we can call it a directory
				exists, err := s.exists(prefix)
				if err != nil {
					return "", err
				}

				valid = !exists
			}

			if valid {
				version = path.Base(prefix)
			}
		}

		if !*resp.IsTruncated {
			break
		} else {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1].String()
		}
	}

	if version != "" {
		return version, nil
	} else {
		return "", fmt.Errorf("No valid versions at %s", s.displayURL(s.path))

	}
}

func (s *S3Backend) ListFiles(version string) ([]string, error) {
	versionPrefix := path.Join(s.path, version)
	marker := ""
	res := make([]string, 0)

	for {
		params := &s3.ListObjectsInput{
			Bucket:		aws.String(s.bucket),
			Delimiter:	aws.String(""),
			Marker:		aws.String(marker),
			MaxKeys:	aws.Int64(1000),
			Prefix:		aws.String(versionPrefix),
		}
		resp, err := s.svc.ListObjects(params)

		if err != nil {
			return nil, s.s3error(err)
		} else if resp.Contents == nil || len(resp.Contents) == 0 {
			break
		}

		for _, key := range resp.Contents {
			name := path.Base(*key.Key)
			// S3 sometimes has keys that are the same as the "directory"
			if strings.TrimSpace(name) != "" && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, ".") {
				res = append(res, name)
			}
		}

		if *resp.IsTruncated {
			marker = resp.CommonPrefixes[len(resp.CommonPrefixes)-1].String()
		} else {
			break
		}
	}

	return res, nil
}

func (s *S3Backend) Open(version, file string) (io.ReadCloser, error) {
	src := path.Join(s.path, version, file)
	params := &s3.GetObjectInput{
		Bucket:                     aws.String(s.bucket),
		Key:                        aws.String(src),
	}
	resp, err := s.svc.GetObject(params)

	if err != nil {
		return nil, fmt.Errorf("Error opening S3 path %s: %s", s.path, err)
	}

	return resp.Body, nil
}

func (s *S3Backend) DisplayPath(version string) string {
	return s.displayURL(s.path, version)
}

func (s *S3Backend) displayURL(pathElements ...string) string {
	key := strings.TrimPrefix(path.Join(pathElements...), "/")
	return fmt.Sprintf("s3://%s/%s", s.bucket, key)
}

func (s *S3Backend) exists(key string) (bool, error) {
	params := &s3.GetObjectInput{
		Bucket:                     aws.String(s.bucket),
		Key:                        aws.String(key),
	}
	resp, err := s.svc.GetObject(params)

	if err != nil {
		return false, s.s3error(err)
	}

	if *resp.ContentLength > int64(0) {
		return true, err
	}
	// TODO: will false always be attached to an err?
	return false, err
}

func (s *S3Backend) s3error(err error) error {
	return fmt.Errorf("Unexpected S3 error on bucket %s: %s", s.bucket, err)
}
