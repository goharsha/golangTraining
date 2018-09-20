package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	cluster "github.com/bsm/sarama-cluster"
)

// Struct to read JSON from topic
type sca struct {
	Log log1
}

type log1 struct {
	Notes        notes
	TraceContext string
	Message      string
	Timestamp    string
}

type notes struct {
	RmsSkuID string
	Output   output
	Result   bool
}

type output struct {
	Rulelog map[string]interface{}
}

//struct to create a specific JSON to update dynamodb
type RuleLog struct {
	Rules []Rule
}

type Rule struct {
	Name   string
	Stores []interface{}
}

var f sca
var r RuleLog
var r1 Rule
var r2 []Rule

var rulesE []string
var AllrulesE []string
var rulesNE []string

var counter = 0

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	ses, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")})
	check(err)

	svc := dynamodb.New(ses)

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// init consumer
	//brokers := []string{"kafka.nonprod.proton.r53.nordstrom.net:9092"}
	brokers := []string{"kafka-ci.sca.nonprod.aws.cloud.r53.nordstrom.net:9092"}
	topics := []string{"atpservice-logs"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group5", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				/*		fmt.Println("******************Raw message***************************")
						fmt.Println()
						fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value) */
				consumer.MarkOffset(msg, "") // mark message as processed
				json.Unmarshal(msg.Value, &f)
				/*		fmt.Println()

						fmt.Println("******************After parsing*************************")
						fmt.Println()

						fmt.Printf("RmsSkuId : %s\n", f.Log.Notes.RmsSkuID)
						fmt.Printf("TraceContext : %s\n", f.Log.TraceContext)
						fmt.Printf("Message type : %s\n", f.Log.Message)
						fmt.Printf("Item available : %v\n", f.Log.Notes.Result)
						fmt.Printf("Timestamp of the message : %s\n", f.Log.Timestamp)

						fmt.Println()

						/*	fmt.Println("RuleLog")
							fmt.Println(f.Log.Notes.Output.Rulelog)*/

				//Logic to create the new JSON structure

				if f.Log.Message == "AuditLog" && strings.HasPrefix(f.Log.TraceContext, "debugflag_") {

					for k, v := range f.Log.Notes.Output.Rulelog {

						switch s1 := v.(type) {
						case []interface{}:
							//	fmt.Println(k, ":")
							//	fmt.Println(s1)
							if k != "RulesExecuted" {
								r1.Name = k
								r1.Stores = s1
								r2 = append(r2, r1)
								rulesE = append(rulesE, k)
							}
							for _, z := range s1 {
								/*		if k == "DropShipLocationEligibilityByFTAC" {
										g := int(z.(float64))
												fmt.Println(g)
										continue
									}*/
								if k == "RulesExecuted" {
									AllrulesE = append(AllrulesE, z.(string))
								}
								//		fmt.Println(z)
							}

						case string:
							//	fmt.Println("It is a string!!")
							//	fmt.Println(k, ":", s1)
						}
					}

					l := len(rulesE)
					var ifound = false
					for _, v := range AllrulesE {
						ifound = false
						for i := 0; i < l; i++ {
							if rulesE[i] == v {
								ifound = true
							}
						}
						if ifound == false {
							rulesNE = append(rulesNE, v)
						}
					}

					for _, j := range rulesNE {
						//	fmt.Println(j)
						r1.Name = j
						r1.Stores = nil
						r2 = append(r2, r1)
					}
					r.Rules = r2

					//To display the final output JSON that is going to update to dynamoDB
					/*	fmt.Println()
						fmt.Println("Final output JSON :")
						fmt.Println()
						jj, err := json.Marshal(r)
						os.Stdout.Write(jj)
						fmt.Println()*/

					//update the JSON to dynamoDB
					var av = make(map[string]*dynamodb.AttributeValue)

					av["Rules"], err = dynamodbattribute.Marshal(r.Rules)
					av["TraceContext"], err = dynamodbattribute.Marshal(f.Log.TraceContext)
					av["RmsSkuID"], err = dynamodbattribute.Marshal(f.Log.Notes.RmsSkuID)
					av["ItemAvailable"], err = dynamodbattribute.Marshal(f.Log.Notes.Result)
					av["Timestamp"], err = dynamodbattribute.Marshal(f.Log.Timestamp)

					check(err)

					params := &dynamodb.PutItemInput{
						Item:      av,
						TableName: aws.String("DebugTool"),
					}

					resp, err := svc.PutItem(params)
					check(err)
					fmt.Println(resp)

					fmt.Printf("Record updated to dynamoDB : %s\n", f.Log.TraceContext)
				}

				rulesE = nil
				rulesNE = nil
				AllrulesE = nil
				r = RuleLog{}
				r1 = Rule{}
				r2 = []Rule{}
				f = sca{}

			}

		case <-signals:
			break ConsumerLoop
			return
		}
	}

}
