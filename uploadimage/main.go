package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"io/ioutil"
	"net/http"
)

// Response is of type APIGatewayProxyResponse since we're leveraging the
// AWS Lambda Proxy Request functionality (default behavior)
//
// https://serverless.com/framework/docs/providers/aws/events/apigateway/#lambda-proxy-integration

func uploadImage(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

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
	_, err = ioutil.ReadAll(file)

	fmt.Println("Header: Filename, ", header.Filename)
	fmt.Println("Header: Filesize, ", header.Size)
	fmt.Println("Header: Filetype, ", header.Header.Get("Content-Type"))

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
	lambda.Start(uploadImage)
}
