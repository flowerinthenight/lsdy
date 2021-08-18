package main

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/flowerinthenight/libdy"
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
	contains []string
	limit    int64
	describe bool
	nosort   bool
	noborder bool
	del      bool
	csvf     string
	b64dec   []string
	maxlen   int

	rootCmd = &cobra.Command{
		Use:   "lsdy <table>",
		Short: "dynamodb query tool",
		Long: `DynamoDB query tool.

To authenticate to AWS, you can set the following environment variables:
  [required]
  AWS_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY

  [optional]
  ROLE_ARN

You can also specify them using the provided flags (see -h). If ROLE_ARN (--rolearn)
is specified, this tool will assume that role using the provided key/secret pair.

To query multiple pk/sk combinations, you can add more --pk flags with its corresponding
--sk inputs (same index).`,
		RunE: run,
	}
)

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
	var cw *csv.Writer
	if csvf != "" {
		f, err = os.Create(fmt.Sprintf("%v", csvf))
		if err != nil {
			return err
		}

		cw = csv.NewWriter(f)
		defer func() {
			cw.Flush()
			f.Close()
		}()
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetColWidth(maxlen)
	if noborder {
		table.SetBorder(false)
		table.SetHeaderLine(false)
		table.SetColumnSeparator("")
		table.SetTablePadding("  ")
		table.SetNoWhiteSpace(true)
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
				tmp, err = libdy.GetItems(svc, args[0], v, vv, limit)
			} else {
				tmp, err = libdy.GetItems(svc, args[0], v, vv)
			}

			if err != nil {
				return err
			}

			// Accumulate results to items.
			items = append(items, tmp...)
		}
	} else {
		if limit > 0 {
			items, err = libdy.ScanItems(svc, args[0], limit)
		} else {
			items, err = libdy.ScanItems(svc, args[0])
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

		// If describe, we're done at this point.
		return nil
	}

	var hdrs []string
	for _, v := range sortedlbl {
		hdrs = append(hdrs, fmt.Sprintf("%v", v))
	}

	table.SetHeader(hdrs)
	if csvf != "" {
		cw.Write(hdrs)
	}

	todel := make(map[string]string) // key=sk, val=pk
	for _, maps := range m {
		include := true
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

			for _, fltr := range contains {
				cc := strings.Split(fltr, ":") // '0:[[!]regex:]expr'
				switch {
				case len(cc) == 2: // not regex
					idx, _ := strconv.Atoi(cc[0])
					if idx == i {
						if cc[1][0] == '^' {
							if strings.Contains(row, cc[1][1:]) {
								include = false
							}
						} else {
							include = false
							if strings.Contains(row, cc[1]) {
								include = true
							}
						}
					}
				case len(cc) == 3: // regex version
					idx, _ := strconv.Atoi(cc[0])
					if idx == i {
						re := regexp.MustCompile(cc[2])
						match := re.MatchString(row)
						switch cc[1] {
						case "^regex":
							include = !match
						case "regex":
							include = match
						}
					}
				}
			}

			rows = append(rows, row)
			if len(row) > maxlen {
				row = row[:maxlen]
			}

			row = strings.Replace(row, "\"", "'", -1)
			qrows = append(qrows, fmt.Sprintf("%v", row))
		}

		if !include {
			continue
		}

		table.Append(rows)
		if csvf != "" {
			cw.Write(qrows)
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
			err = libdy.DeleteItem(svc, args[0], pklbl+":"+v, sklbl+":"+k)
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
	rootCmd.Flags().StringSliceVar(&contains, "contains", contains, "filter output, '^' means exclude, fmt: <col-index:[[^]regex:]expr>, i.e. '1:^regex:my.*'")
	rootCmd.Flags().BoolVar(&describe, "describe", describe, "if set, describe the table only")
	rootCmd.Flags().Int64Var(&limit, "limit", limit, "max number of output for query/scan")
	rootCmd.Flags().BoolVar(&nosort, "nosort", nosort, "if set, don't sort the attributes")
	rootCmd.Flags().BoolVar(&noborder, "noborder", noborder, "if set, remove table borders")
	rootCmd.Flags().BoolVar(&del, "delete", del, "if set, delete the items that are queried")
	rootCmd.Flags().StringVar(&csvf, "csv", csvf, "if provided, output to csv with value as filename")
	rootCmd.Flags().IntVar(&maxlen, "maxlen", tablewriter.MAX_ROW_WIDTH, "max len of each cell")
	rootCmd.Flags().StringSliceVar(&b64dec, "decb64", b64dec, "decode base64-encoded sections, fmt: <col-index[:sep:split-index]>, i.e. '1', '1:|:3'")
	rootCmd.Execute()
}
