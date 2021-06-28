package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"net/http"
)

var svc *dynamodb.DynamoDB
var uploader *s3manager.Uploader

func init() {
	// Connect to dynamoDB
	sess := session.Must(session.NewSession())

	svc = dynamodb.New(sess)
	uploader = s3manager.NewUploader(sess)

}
func uploadImageTos3(sha256String string, calculatedHashes []byte, contentType string) error {

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String("watermark-image-bucket"),
		Key:         aws.String(sha256String),
		Body:        bytes.NewReader(calculatedHashes),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))
	return nil
}
func uploadImageHandler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	// Go ahead and create the req to a http request
	r := http.Request{}
	r.Header = make(map[string][]string)
	for k, v := range req.Headers {
		if k == "content-type" || k == "Content-Type" {
			r.Header.Set(k, v)
		}
	}
	// NOTE: API Gateway is set up with */* as binary media type, so all APIGatewayProxyRequests will be base64 encoded
	r.Body = ioutil.NopCloser(bytes.NewBuffer([]byte(req.Body)))
	fmt.Println("Converted to request")

	// We should be able to gather the data as usual now.
	file, header, err := r.FormFile("file")
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       err.Error(),
		}, err
	}
	defer file.Close()
	imageBytes, err := ioutil.ReadAll(file)

	fmt.Println("Header: Filename, ", header.Filename)
	fmt.Println("Header: Filesize, ", header.Size)
	fmt.Println("Header: Filetype, ", header.Header.Get("Content-Type"))

	// Calculate the MD5 checksum
	var calculatedHash = md5.Sum(imageBytes)
	fmt.Printf("%x\n", calculatedHash)
	sha256 := sha256.Sum256(imageBytes)
	fmt.Printf("%x\n", sha256)
	var sha256String = hex.EncodeToString(sha256[:])

	uploadImageTos3(sha256String, imageBytes, header.Header.Get("Content-Type"))
	resp := events.APIGatewayProxyResponse{
		StatusCode:      200,
		IsBase64Encoded: false,
		Body:            "Uploaded: " + header.Filename,
		Headers: map[string]string{
			"Content-Type":           "application/json",
			"X-MyCompany-Func-Reply": "uploadimage-handler",
		},
	}

	return resp, nil
}

func main() {
	lambda.Start(uploadImageHandler)
}
