package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/cenkalti/backoff"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var (
	region   string
	key      string
	secret   string
	rolearn  string
	pk       []string
	sk       []string
	incols   []string
	limit    int64
	describe bool
	nosort   bool
	noborder bool
	del      bool
	csv      string
	sep      string
	b64dec   []string
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
is specified, this tool will assume that role using the provided key/secret pair.

To query multiple pk/sk combinations, you can add more --pk flags with its corresponding
--sk inputs (same index).`,
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

		if input.Limit != nil {
			if int64(len(ret)) >= *input.Limit {
				more = false
				lastKey = nil
			}
		}
	}

	return ret, nil
}

func GetItems(svc *dynamodb.DynamoDB, table, pk, sk string, limit ...int64) ([]map[string]*dynamodb.AttributeValue, error) {
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

		if len(limit) > 0 {
			input.Limit = aws.Int64(limit[0])
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

		if len(limit) > 0 {
			input.Limit = aws.Int64(limit[0])
		}
	}

	return query(svc, table, input)
}

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

func ScanItems(svc *dynamodb.DynamoDB, table string, limit ...int64) ([]map[string]*dynamodb.AttributeValue, error) {
	start := time.Now()
	ret := []map[string]*dynamodb.AttributeValue{}
	var lastKey map[string]*dynamodb.AttributeValue
	more := true

	in := dynamodb.ScanInput{TableName: aws.String(table)}
	if len(limit) > 0 {
		in.Limit = aws.Int64(limit[0])
	}

	// Could be paginated.
	for more {
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

		if in.Limit != nil {
			if int64(len(ret)) >= *in.Limit {
				more = false
				lastKey = nil
			}
		}
	}

	return ret, nil
}

func DeleteItem(svc *dynamodb.DynamoDB, table, pk, sk string) error {
	v1 := strings.Split(pk, ":")
	v2 := strings.Split(sk, ":")
	start := time.Now()
	var input *dynamodb.DeleteItemInput
	if sk == "" {
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				v1[0]: {S: aws.String(v1[1])},
			},
		}
	} else {
		input = &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				v1[0]: {S: aws.String(v1[1])},
				v2[0]: {S: aws.String(v2[1])},
			},
		}
	}

	var rerr error

	// Our retriable function.
	op := func() error {
		_, err := svc.DeleteItem(input)
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

	err := backoff.Retry(op, backoff.NewExponentialBackOff())
	if err != nil {
		return fmt.Errorf("DeleteItem failed after %v: %w", time.Since(start), err)
	}

	if rerr != nil {
		return fmt.Errorf("DeleteItem failed: %w", rerr)
	}

	return nil
}

func run(cmd *cobra.Command, args []string) error {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	if len(args) == 0 {
		return fmt.Errorf("<table> cannot be empty")
	}

	// Validate pk and sk inputs.
	var pklbl, sklbl string
	for _, v := range pk {
		if v != "" {
			if !strings.Contains(v, ":") {
				return fmt.Errorf("invalid --pk format: %v", v)
			}

			// Expected to be the same across all inputs.
			pklbl = strings.Split(v, ":")[0]
		}
	}

	for _, v := range sk {
		if v != "" {
			if !strings.Contains(v, ":") {
				return fmt.Errorf("invalid --sk format: %v", v)
			}

			// Expected to be the same across all inputs.
			sklbl = strings.Split(v, ":")[0]
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
	if csv != "" {
		f, err = os.Create(fmt.Sprintf("%v", csv))
		if err != nil {
			return err
		}

		defer f.Close()
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetColWidth(maxlen)
	if noborder {
		table.SetBorder(false)
		table.SetHeaderLine(false)
		table.SetColumnSeparator(" ")
		table.SetTablePadding("")
	}

	// Get table information.
	t, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(args[0])})
	if err != nil {
		return err
	}

	for _, v := range t.Table.KeySchema {
		if *v.KeyType == "HASH" {
			pklbl = *v.AttributeName
		}

		if *v.KeyType == "RANGE" {
			sklbl = *v.AttributeName
		}
	}

	if describe {
		log.Println(t)
		log.Println("")
	}

	var items []map[string]*dynamodb.AttributeValue
	var m []map[string]interface{}
	if len(pk) > 0 {
		for i, v := range pk {
			var vv string
			if len(sk) > 0 {
				if i <= len(sk)-1 {
					vv = sk[i]
				}
			}

			var tmp []map[string]*dynamodb.AttributeValue
			if limit > 0 {
				tmp, err = GetItems(svc, args[0], v, vv, limit)
			} else {
				tmp, err = GetItems(svc, args[0], v, vv)
			}

			if err != nil {
				return err
			}

			// Accumulate results to items.
			items = append(items, tmp...)
		}
	} else {
		if limit > 0 {
			items, err = ScanItems(svc, args[0], limit)
		} else {
			items, err = ScanItems(svc, args[0])
		}

		if err != nil {
			return err
		}
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

	if !nosort {
		sort.Strings(sortedlbl)
	}

	if describe {
		log.Println("Attributes:")
		for _, v := range sortedlbl {
			log.Println("-", v)
		}
		return nil
	}

	var hdrs []string
	var qhdrs []string
	for _, v := range sortedlbl {
		hdrs = append(hdrs, fmt.Sprintf("%v", v))
		qhdrs = append(qhdrs, fmt.Sprintf("\"%v\"", v))
	}

	table.SetHeader(hdrs)
	if csv != "" {
		fmt.Fprintf(f, strings.Join(qhdrs, sep))
		fmt.Fprintf(f, "\n")
	}

	todel := make(map[string]string) // key=sk, val=pk
	for _, maps := range m {
		var rows []string
		var qrows []string
		for i, k := range sortedlbl {
			if _, ok := maps[k]; !ok {
				rows = append(rows, "-")
				qrows = append(qrows, "-")
				continue
			}

			row := fmt.Sprintf("%v", maps[k])
			for _, decv := range b64dec {
				sp := strings.Split(decv, ":")
				switch {
				case len(sp) == 1: // '0', '2', ...
					idx, _ := strconv.Atoi(sp[0])
					if idx == i {
						data, err := base64.StdEncoding.DecodeString(row)
						if err == nil {
							row = string(data)
						}
					}
				case len(sp) == 3: // '1:|:3'
					idx, _ := strconv.Atoi(sp[0])
					sidx, _ := strconv.Atoi(sp[2])
					if idx == i {
						sr := strings.Split(row, sp[1])
						if len(sr) > 1 && sidx < len(sr) {
							data, err := base64.StdEncoding.DecodeString(sr[sidx])
							if err == nil {
								sr[sidx] = string(data)
								row = strings.Join(sr, sp[1])
							}
						}
					}
				}
			}

			if len(row) > maxlen {
				row = row[:maxlen]
			}

			rows = append(rows, row)
			row = strings.Replace(row, "\"", "'", -1)
			qrows = append(qrows, fmt.Sprintf("\"%v\"", row))
		}

		table.Append(rows)
		if csv != "" {
			fmt.Fprintf(f, strings.Join(qrows, sep))
			fmt.Fprintf(f, "\n")
		}

		// Setup the items to delete, if set.
		if del {
			if _, ok := maps[sklbl]; ok {
				todel[fmt.Sprintf("%v", maps[sklbl])] = fmt.Sprintf("%v", maps[pklbl])
			}
		}
	}

	// Final table render.
	table.Render()

	// If there are items to delete.
	if del {
		for k, v := range todel {
			err = DeleteItem(svc, args[0], pklbl+":"+v, sklbl+":"+k)
			if err != nil {
				log.Printf("delete failed: [key:%v, sortkey:%v] %v\n", v, k, err)
			} else {
				log.Printf("deleted: key:%v, sortkey:%v\n", v, k)
			}
		}
	}

	return nil
}

func main() {
	rootCmd.Flags().SortFlags = false
	rootCmd.Flags().StringVar(&region, "region", os.Getenv("AWS_REGION"), "region")
	rootCmd.Flags().StringVar(&key, "key", os.Getenv("AWS_ACCESS_KEY_ID"), "access key")
	rootCmd.Flags().StringVar(&secret, "secret", os.Getenv("AWS_SECRET_ACCESS_KEY"), "secret access key")
	rootCmd.Flags().StringVar(&rolearn, "rolearn", os.Getenv("ROLE_ARN"), "if set, the role to assume using the provided key/secret")
	rootCmd.Flags().StringSliceVar(&pk, "pk", pk, "primary key to query, format: [key:value] (if empty, scan is implied)")
	rootCmd.Flags().StringSliceVar(&sk, "sk", sk, "sort key if any, format: [key:value] (begins_with will be used if not empty)")
	rootCmd.Flags().StringSliceVar(&incols, "attr", incols, "attributes (columns) to include")
	rootCmd.Flags().BoolVar(&describe, "describe", describe, "if set, describe the table only")
	rootCmd.Flags().Int64Var(&limit, "limit", limit, "max number of output for query/scan")
	rootCmd.Flags().BoolVar(&nosort, "nosort", nosort, "if set, don't sort the attributes")
	rootCmd.Flags().BoolVar(&noborder, "noborder", noborder, "if set, remove table borders")
	rootCmd.Flags().BoolVar(&del, "delete", del, "if set, delete the items that are queried")
	rootCmd.Flags().StringVar(&csv, "csv", csv, "if provided, output to csv with value as filename")
	rootCmd.Flags().StringVar(&sep, "sep", ",", "csv separator")
	rootCmd.Flags().IntVar(&maxlen, "maxlen", tablewriter.MAX_ROW_WIDTH, "max len of each cell")
	rootCmd.Flags().StringSliceVar(&b64dec, "decb64", b64dec, "decode base64-encoded sections, fmt: <col-index[:sep:split-index]>, i.e. '1', '1:|:3'")
	rootCmd.Execute()
}
