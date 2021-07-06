package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http"
	"os"
	"runtime"
	"strconv"
)

var svc *dynamodb.Client
var uploader *s3.Client
var psClient *s3.PresignClient

type Image struct {
	ImageID   string
	UserID    string
	Size      int64
	Filename  string
	ImageType string
	SignedURL string
}

func Map(vs []Image, f func(Image) Image) []Image {
	vsm := make([]Image, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

// S3PresignGetObjectAPI defines the interface for the PresignGetObject function.
// We use this interface to test the function using a mocked service.
type S3PresignGetObjectAPI interface {
	PresignGetObject(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

func init() {
	// Connect to dynamoDB
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	svc = dynamodb.NewFromConfig(cfg)
	uploader = s3.NewFromConfig(cfg)
	psClient = s3.NewPresignClient(uploader)

}
func GetPresignedURL(c context.Context, api S3PresignGetObjectAPI, input *s3.GetObjectInput) (*v4.PresignedHTTPRequest, error) {
	return api.PresignGetObject(c, input)
}

// RetrieveSignedURL TODO: Sends out the request for the signed url untill I figure out how to send through channels.
func RetrieveSignedURL(image Image, c chan Image) {
	// Take the number of requests to fan out each
	input := &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key:    aws.String(image.ImageID),
	}

	presignResp, err := GetPresignedURL(context.TODO(), psClient, input)
	if err != nil {
		fmt.Println("Got an error retrieving pre-signed object:")
		fmt.Println(err)

	}
	c <- Image{
		SignedURL: presignResp.URL,
		UserID:    image.UserID,
		Size:      image.Size,
		Filename:  image.Filename,
		ImageType: image.ImageType,
		ImageID:   image.ImageID,
	}

	fmt.Println("The URL form goRoutine:", presignResp.URL)
}
func Handler(ctx context.Context) (events.APIGatewayProxyResponse, error) {
	resp, err := svc.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(os.Getenv("TABLE_NAME")),
		KeyConditionExpression: aws.String("UserID = :hashKey "),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":hashKey": &types.AttributeValueMemberS{Value: "123"}},
	})

	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error while scanning DynamoTable" + err.Error(),
		}, nil
	}

	// Pull out only the data we need
	var images []Image
	err = attributevalue.UnmarshalListOfMaps(resp.Items, &images)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal Dynamodb Scan Items, %v", err))
	}

	// JSON string of movies to return
	numImages := strconv.Itoa(len(images))
	fmt.Println("Found: ", numImages, "images for the UserID: ", "123")

	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 404}, err
	}

	println("Number of vcpus: ", runtime.NumCPU())
	// Number of async futures
	c := make(chan Image)
	// Fire off routines
	for _, image := range images {
		go RetrieveSignedURL(image, c)
	}
	// Collect the results
	result := make([]Image, len(images))
	for i, _ := range result {
		result[i] = <-c
		fmt.Println("Fan in: ", result[i].SignedURL)
	}
	imagesResults, err := json.Marshal(result)
	handlerResp := events.APIGatewayProxyResponse{
		StatusCode:      200,
		IsBase64Encoded: false,
		Body:            string(imagesResults),
		Headers: map[string]string{
			"Content-Type":           "application/json",
			"X-MyCompany-Func-Reply": "findall-handler",
		},
	}

	return handlerResp, nil
}

func main() {
	lambda.Start(Handler)
}
