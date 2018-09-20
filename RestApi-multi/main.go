package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type RuleLog struct {
	ItemAvailable bool
	RmsSkuID      string
	Timestamp     string
	TraceContext  string
	Rules         []Rule
}

type Rule struct {
	Name   string
	Stores []interface{}
}

var r RuleLog
var svc dynamodb.DynamoDB

func main() {
	fmt.Println("Hello world!")

	/*	ses, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")})
		check(err)

		svc := dynamodb.New(ses)*/

	//Rest-api implementation
	router := mux.NewRouter()
	router.HandleFunc("/getDetails/{guid}", GetDetails).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))

	/*	val, err := ses.Config.Credentials.Get()
		fmt.Println("AccessKeyId : " + val.AccessKeyID)
		fmt.Println("Session token : " + val.SessionToken)
		fmt.Println("ProvoderName : " + val.ProviderName)
		fmt.Println("Secret Access key : " + val.SecretAccessKey)*/

	//svc := dynamodb.New(ses)

	//creating a new table
	/*	input := &dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("TraceContext"),
					AttributeType: aws.String("S"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("TraceContext"),
					KeyType:       aws.String("HASH"),
				},
			},

			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
			TableName: aws.String("DebugTool"),
		}

		result, err := svc.CreateTable(input)
		check(err)
		fmt.Println(result)*/

	//reading an item from dynamodb Table
	/*	params := &dynamodb.GetItemInput{
			TableName: aws.String("DebugTool"),
			Key: map[string]*dynamodb.AttributeValue{
				"TraceContext": {
					S: aws.String("streamaudit_93367ed5-36e7-48bd-448f-0eb9860baa8d|fc06020b-05a7-43f6-442e-35e7e1864240"),
				},
			},
			//	ProjectionExpression: aws.String("RuleLog"),
		}

		result, err := svc.GetItem(params)
		check(err)

		fmt.Println("Output from dynamoDB")
		fmt.Println(result)

		err = dynamodbattribute.UnmarshalMap(result.Item, &r)

		check(err)

		fmt.Println("***After UnmarshalMap***")
		fmt.Println(r)

		fmt.Println()

		jj, err := json.Marshal(r)
		os.Stdout.Write(jj)*/
}

func GetDetails(w http.ResponseWriter, r *http.Request) {
	var guid string
	var rl RuleLog
	params := mux.Vars(r)

	//fmt.Println("value from the request : ")
	//	fmt.Println(params)
	for k, v := range params {
		fmt.Println(k, ":", v)
		guid = v
	}
	rl = ExtractJSONFromDynamoDB(guid)
	if rl.RmsSkuID == "" {
		fmt.Println("Nothing from the DB")
		http.Error(w, "No item present", 500)
	} else {
		json.NewEncoder(w).Encode(rl)
	}
}

func ExtractJSONFromDynamoDB(v string) RuleLog {
	//fmt.Println("Inside this function : " + v)

	ses, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")})
	check(err)

	svc := dynamodb.New(ses)

	params := &dynamodb.GetItemInput{
		TableName: aws.String("DebugTool"),
		Key: map[string]*dynamodb.AttributeValue{
			"TraceContext": {
				S: aws.String(v),
			},
		},
		//	ProjectionExpression: aws.String("RuleLog"),
	}

	result, err := svc.GetItem(params)
	check(err)

	r = RuleLog{}

	//fmt.Println("Output from dynamoDB")
	//	fmt.Println(result)
	if result.Item == nil {
		fmt.Println("Item not present in dynamoDB")
		return r
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &r)

	check(err)

	/*fmt.Println("***After UnmarshalMap***")
	fmt.Println(r)

	fmt.Println()
	fmt.Println("Final output JSON")
	jj, err := json.Marshal(r)
	os.Stdout.Write(jj)
	fmt.Println()*/
	//	r = RuleLog{}
	//	fmt.Println("END******")
	//	fmt.Println(r)
	return r
}
