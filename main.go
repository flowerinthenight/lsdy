package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/cenkalti/backoff"
	"github.com/spf13/cobra"
)

var (
	region   string
	key      string
	secret   string
	rolearn  string
	pk       string
	sk       string
	incols   []string
	excols   []string
	out      string
	describe bool
	maxlen   int

	rootCmd = &cobra.Command{
		Use:   "lsdy <table>",
		Short: "dynamodb query tool",
		Long: `DynamoDB query tool.

To authenticate to AWS, you can set the following environment variables:
  AWS_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  ROLE_ARN

You can also specify them using the provided flags (see -h). If ROLE_ARN (--rolearn)
is specified, this tool will assume that role using the provided key/secret pair.`,
		RunE: run,
	}
)

func query(svc *dynamodb.DynamoDB, table string, input *dynamodb.QueryInput) ([]map[string]*dynamodb.AttributeValue, error) {
	start := time.Now()
	ret := []map[string]*dynamodb.AttributeValue{}
	var lastKey map[string]*dynamodb.AttributeValue
	more := true

	// Could be paginated.
	for more {
		if lastKey != nil {
			input.ExclusiveStartKey = lastKey
		}

		var rerr, err error
		var res *dynamodb.QueryOutput

		// Our retriable, backoff-able function.
		op := func() error {
			res, err = svc.Query(input)
			rerr = err
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case dynamodb.ErrCodeProvisionedThroughputExceededException:
						return err // will cause retry with backoff
					}
				}
			}

			return nil // final err is rerr
		}

		err = backoff.Retry(op, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, fmt.Errorf("query failed after %v: %w", time.Since(start), err)
		}

		if rerr != nil {
			return nil, fmt.Errorf("query failed: %w", rerr)
		}

		ret = append(ret, res.Items...)
		more = false
		if res.LastEvaluatedKey != nil {
			lastKey = res.LastEvaluatedKey
			more = true
		}
	}

	return ret, nil
}

func GetItems(svc *dynamodb.DynamoDB, table, pk, sk string) ([]map[string]*dynamodb.AttributeValue, error) {
	v1 := strings.Split(pk, ":")
	v2 := strings.Split(sk, ":")
	var input *dynamodb.QueryInput
	if sk != "" {
		skexpr := fmt.Sprintf("%v = :pk AND begins_with(%v, :sk)", v1[0], v2[0])
		input = &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String(skexpr),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":pk": {S: aws.String(v1[1])},
				":sk": {S: aws.String(v2[1])},
			},
			ScanIndexForward: aws.Bool(false), // descending order
		}
	} else {
		input = &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String(fmt.Sprintf("%v = :pk", v1[0])),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":pk": {S: aws.String(v1[1])},
			},
			ScanIndexForward: aws.Bool(false), // descending order
		}
	}

	return query(svc, table, input)
}

// GetGsiItems is a generic function for reading items using a GSI.
func GetGsiItems(svc *dynamodb.DynamoDB, table, index, key, value string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := dynamodb.QueryInput{
		TableName:              aws.String(table),
		IndexName:              aws.String(index),
		KeyConditionExpression: aws.String(fmt.Sprintf("%v = :v", key)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {S: aws.String(value)},
		},
	}

	return query(svc, table, &input)
}

func ScanItems(svc *dynamodb.DynamoDB, table string) ([]map[string]*dynamodb.AttributeValue, error) {
	start := time.Now()
	ret := []map[string]*dynamodb.AttributeValue{}
	var lastKey map[string]*dynamodb.AttributeValue
	more := true

	// Could be paginated.
	for more {
		in := dynamodb.ScanInput{TableName: aws.String(table)}
		if lastKey != nil {
			in.ExclusiveStartKey = lastKey
		}

		var rerr, err error
		var res *dynamodb.ScanOutput

		// Our retriable, backoff-able function.
		op := func() error {
			res, err = svc.Scan(&in)
			rerr = err
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case dynamodb.ErrCodeProvisionedThroughputExceededException:
						return err // will cause retry with backoff
					}
				}
			}

			return nil // final err is rerr
		}

		err = backoff.Retry(op, backoff.NewExponentialBackOff())
		if err != nil {
			return nil, fmt.Errorf("ScanItems failed after %v: %w", time.Since(start), err)
		}

		if rerr != nil {
			return nil, fmt.Errorf("ScanItems failed: %w", rerr)
		}

		ret = append(ret, res.Items...)
		more = false
		if res.LastEvaluatedKey != nil {
			lastKey = res.LastEvaluatedKey
			more = true
		}
	}

	return ret, nil
}

func run(cmd *cobra.Command, args []string) error {
	log.SetFlags(0)
	if len(args) == 0 {
		return fmt.Errorf("<table> cannot be empty")
	}

	if pk != "" {
		if !strings.Contains(pk, ":") {
			return fmt.Errorf("invalid --pk format: %v", pk)
		}
	}

	if sk != "" {
		if !strings.Contains(sk, ":") {
			return fmt.Errorf("invalid --sk format: %v", sk)
		}
	}

	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	})

	var svc *dynamodb.DynamoDB
	if rolearn != "" {
		cnf := &aws.Config{Credentials: stscreds.NewCredentials(sess, rolearn)}
		svc = dynamodb.New(sess, cnf)
	} else {
		svc = dynamodb.New(sess)
	}

	var err error
	var f *os.File
	if out != "" {
		f, err = os.Create(fmt.Sprintf("%v.csv", out))
		if err != nil {
			return err
		}

		defer f.Close()
	}

	var b bytes.Buffer
	wb := bufio.NewWriter(&b)
	h := tabwriter.NewWriter(wb, 0, 4, 4, ' ', 0)
	defer func() {
		h.Flush()
		wb.Flush()
		fmt.Printf("%v", b.String())
	}()

	addLine := func(w *tabwriter.Writer, v []interface{}) {
		var f string
		for i, vv := range v {
			val := fmt.Sprintf("%v", vv)
			val = strings.TrimSuffix(strings.TrimPrefix(val, " "), " ")
			if val == "" {
				val = "-"
			}

			if len(val) > maxlen {
				val = val[:maxlen]
			}

			f += val
			if i < len(v)-1 {
				f += "\t"
			}
		}

		f += "\n"
		fmt.Fprintf(w, f)
	}

	if describe {
		t, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(args[0])})
		if err != nil {
			return err
		}

		log.Println(t)
	}

	var items []map[string]*dynamodb.AttributeValue
	var m []map[string]interface{}
	if pk != "" {
		items, err = GetItems(svc, args[0], pk, sk)
	} else {
		items, err = ScanItems(svc, args[0])
	}

	err = dynamodbattribute.UnmarshalListOfMaps(items, &m)
	if err != nil {
		return err
	}

	lbl := make(map[string]struct{})
	sortedlbl := []string{}
	if len(incols) > 0 {
		sortedlbl = incols
	} else {
		for _, maps := range m {
			for k, _ := range maps {
				lbl[k] = struct{}{}
			}
		}
	}

	for k, _ := range lbl {
		sortedlbl = append(sortedlbl, k)
	}

	sort.Strings(sortedlbl)
	if describe {
		log.Println("")
		log.Println("Attributes:")
		for _, v := range sortedlbl {
			log.Println("-", v)
		}
		return nil
	}

	var lbls []interface{}
	for _, v := range sortedlbl {
		lbls = append(lbls, v)
	}

	addLine(h, lbls)
	for _, maps := range m {
		var toadd []interface{}
		for _, k := range sortedlbl {
			if _, ok := maps[k]; ok {
				toadd = append(toadd, maps[k])
			} else {
				toadd = append(toadd, "-")
			}
		}

		addLine(h, toadd)
	}

	return nil
}

func main() {
	rootCmd.Flags().SortFlags = false
	rootCmd.Flags().StringVar(&region, "region", os.Getenv("AWS_REGION"), "region")
	rootCmd.Flags().StringVar(&key, "key", os.Getenv("AWS_ACCESS_KEY_ID"), "access key")
	rootCmd.Flags().StringVar(&secret, "secret", os.Getenv("AWS_SECRET_ACCESS_KEY"), "secret access key")
	rootCmd.Flags().StringVar(&rolearn, "rolearn", os.Getenv("ROLE_ARN"), "if not empty, the role to assume using the provided key/secret")
	rootCmd.Flags().StringVar(&pk, "pk", pk, "primary key to query, format: [key:value] (if empty, scan is implied)")
	rootCmd.Flags().StringVar(&sk, "sk", sk, "sort key if any, format: [key:value] (begins_with will be used if not empty)")
	rootCmd.Flags().StringSliceVar(&incols, "attr", incols, "attributes (columns) to include")
	rootCmd.Flags().StringVar(&out, "out", out, "if provided, output to csv with value as filename (.csv appended)")
	rootCmd.Flags().BoolVar(&describe, "describe", describe, "if true, describe the table only")
	rootCmd.Flags().IntVar(&maxlen, "maxlen", 20, "max len of each cell")
	rootCmd.Execute()
}
