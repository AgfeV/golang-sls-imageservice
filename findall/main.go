package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var svc *dynamodb.Client
var uploader *s3manager.Uploader

type Image struct {
	ImageID   string
	UserID    string
	Size      int64
	Filename  string
	ImageType string
	SignedURL string
}

func init() {
	// Connect to dynamoDB
	sess := session.Must(session.NewSession())
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	svc = dynamodb.NewFromConfig(cfg)
	uploader = s3manager.NewUploader(sess)

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
	response, err := json.Marshal(images)
	numImages := strconv.Itoa(len(images))
	fmt.Println("Found: ", numImages, "images for the UserID: ", "123")

	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 404}, err
	}

	handlerResp := events.APIGatewayProxyResponse{
		StatusCode:      200,
		IsBase64Encoded: false,
		Body:            string(response),
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
