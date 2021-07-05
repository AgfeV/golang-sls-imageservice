package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var svc *dynamodb.DynamoDB
var uploader *s3manager.Uploader

type Image struct {
	ImageID    string
	UserID     string
	Size       int64
	Filename   string
	ImageType  string
	ImageBytes []byte
}

func init() {
	// Connect to dynamoDB
	sess := session.Must(session.NewSession())
	svc = dynamodb.New(sess)
	uploader = s3manager.NewUploader(sess)

}
func (imgPtr *Image) uploadImageTos3() error {

	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:      aws.String("watermark-image-bucket"),
		Key:         aws.String(imgPtr.ImageID),
		Body:        bytes.NewBuffer(imgPtr.ImageBytes),
		ContentType: aws.String(imgPtr.ImageType),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))
	return err
}
func (imgPtr *Image) updateImageInDynamo() error {

	var imageRequest interface{} = struct {
		ImageID   string
		UserID    string
		Size      int64
		Filename  string
		ImageType string
	}{
		ImageID:   imgPtr.ImageID,
		UserID:    imgPtr.UserID,
		Size:      imgPtr.Size,
		Filename:  imgPtr.Filename,
		ImageType: imgPtr.ImageType,
	}
	av, err := dynamodbattribute.MarshalMap(imageRequest)

	if err != nil {
		log.Fatalf("Got error marshalling new movie item: %s", err)
	}
	_, err = svc.PutItem(&dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(os.Getenv("TABLE_NAME")),
	})
	return err
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
	body, _ := base64.StdEncoding.DecodeString(req.Body)
	// Copy over the decoded binary image to new manually created http request.
	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	r.ParseMultipartForm(32 << 20) // maxMemory 32MB

	// Retrieve the binary image
	file, header, err := r.FormFile("file")
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       err.Error(),
		}, err
	}
	defer file.Close()

	imageBytes, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("ReadAll error: ", err.Error())
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       err.Error(),
		}, err
	}
	// Calculate the sha256 checksum
	hash := sha256.New()
	if _, err := io.Copy(hash, bytes.NewReader(imageBytes)); err != nil {
		log.Fatal(err)
	}
	sum := hash.Sum(nil)

	fmt.Printf("%x\n", sum)
	fmt.Println("Header: Filename, ", header.Filename)
	fmt.Println("Header: Filesize, ", header.Size)
	fmt.Println("Header: Filetype, ", header.Header.Get("Content-Type"))
	var sha256String = hex.EncodeToString(sum[:])

	var imageObject = Image{
		ImageID:    sha256String,
		UserID:     "123",
		Size:       header.Size,
		ImageBytes: imageBytes,
		Filename:   header.Filename,
		ImageType:  header.Header.Get("Content-Type"),
	}

	dynamoUploadError := imageObject.updateImageInDynamo()
	if dynamoUploadError != nil {
		fmt.Println("update dynamodb image Error: ", dynamoUploadError.Error())
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       dynamoUploadError.Error(),
		}, dynamoUploadError
	}

	s3UploadErr := imageObject.uploadImageTos3()
	if s3UploadErr != nil {
		fmt.Println("Upload to s3 bucket Error: ", s3UploadErr.Error())
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       s3UploadErr.Error(),
		}, s3UploadErr
	}

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
