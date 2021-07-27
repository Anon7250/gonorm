// Use AWS Dynamodb as storage
package gonorm

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	dyndb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyndbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gofiber/fiber/v2"
)

const TableKey = "key"
const TableJsonField = "rawJson"
const TableStrListField = "strList"
const StrListCreatedMarker = "<CREATED>"
const ConditionKeyDoesntExist = "attribute_not_exists(#key)"
const ConditionKeyExists = "attribute_exists(#key)"
const AppendToStrListExpr = "SET strList = list_append(strList, :AppendItems)"
const RenameNewItems = ":AppendItems"
const RenameTableKey = "#key"
const RequestTokenSize = 36

type DynDB struct {
	AwsConfig *aws.Config
	DB        *dyndb.Client
	Table     string
}

func NewDynDB(table string) (*DynDB, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	db := dyndb.NewFromConfig(cfg)
	return &DynDB{AwsConfig: &cfg, DB: db, Table: table}, nil
}

func (todo *DynDB) getItem(key string) (*dyndb.GetItemOutput, error) {
	input := dyndb.GetItemInput{
		Key: map[string]dyndbTypes.AttributeValue{
			TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
		},
		TableName: aws.String(todo.Table),
	}
	return todo.DB.GetItem(context.TODO(), &input)
}

func (todo *DynDB) HasKey(key string) (bool, error) {
	errMsg := "Cannot read from database: " + key + ". "
	result, err := todo.getItem(key)
	if err != nil {
		return false, fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
	}
	hasKey := result.Item != nil
	return hasKey, nil
}

func (todo *DynDB) GetJson(key string, valueOut interface{}) error {
	result, err := todo.getItem(key)
	if err != nil {
		return err
	}
	if result.Item == nil {
		return fiber.NewError(fiber.StatusNotFound)
	}

	errMsg := "Cannot read JSON from database: " + key + ". "
	rawJson, ok := result.Item[TableJsonField]
	if !ok {
		return fiber.NewError(fiber.StatusInternalServerError, errMsg)
	}

	err = attributevalue.Unmarshal(rawJson, valueOut)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
	}
	return nil
}

func (todo *DynDB) GetJsons(keys []string, rawValuesOut *[]interface{}) error {
	keyFilters := make([]map[string]dyndbTypes.AttributeValue, 0)
	for _, key := range keys {
		filter := map[string]dyndbTypes.AttributeValue{
			TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
		}
		keyFilters = append(keyFilters, filter)
	}
	input := dyndb.BatchGetItemInput{
		RequestItems: map[string]dyndbTypes.KeysAndAttributes{
			todo.Table: {
				Keys: keyFilters,
			},
		},
	}
	output, err := todo.DB.BatchGetItem(context.TODO(), &input)
	if err != nil {
		return fiber.NewError(
			fiber.StatusInternalServerError,
			fmt.Sprintf("Cannot fetch jsons from database: %v", err),
		)
	}

	*rawValuesOut = make([]interface{}, 0)
	for _, item := range output.Responses[todo.Table] {
		asMap := dyndbTypes.AttributeValueMemberM{
			Value: item,
		}
		*rawValuesOut = append(*rawValuesOut, asMap)
	}
	return nil
}

func (todo *DynDB) Unmarshal(rawValue interface{}, valueOut interface{}) error {
	asMap, ok := rawValue.(dyndbTypes.AttributeValueMemberM)
	if !ok {
		return fiber.NewError(
			fiber.StatusInternalServerError,
			fmt.Sprintf("Expecting %v to be stored in AWS format.", rawValue),
		)
	}
	rawJson, ok := asMap.Value["rawJson"]
	if !ok {
		return fiber.NewError(
			fiber.StatusInternalServerError,
			fmt.Sprintf("Expecting %v to contain 'rawJson'", asMap.Value["key"]),
		)
	}
	return attributevalue.Unmarshal(rawJson, valueOut)
}

func (todo *DynDB) GetStringList(key string, valueOut *[]string) error {
	result, err := todo.getItem(key)
	if err != nil {
		return err
	}
	if result.Item == nil {
		return fiber.NewError(fiber.StatusNotFound)
	}

	errMsg := "Cannot read a list from database: " + key + ". "
	rawStrList, ok := result.Item[TableStrListField]
	if !ok {
		return fiber.NewError(fiber.StatusInternalServerError, errMsg)
	}

	err = attributevalue.Unmarshal(rawStrList, valueOut)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, errMsg)
	}
	return nil
}

func (todo *DynDB) DoWriteTransaction(t WriteTransaction) error {
	errMsg := "Failed to write to database: "
	uuidStr, err := GetUUID()
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, errMsg+"Cannot generate UUID for transaction requests")
	}

	transactions := make([]dyndbTypes.TransactWriteItem, 0)
	createsKeys := make(map[string]bool)
	createsStrLists := make(map[string]bool)
	for key := range t.Creates {
		createsKeys[key] = true
	}
	for _, key := range t.StrListCreates {
		createsKeys[key] = true
		createsStrLists[key] = true
	}
	for key := range createsKeys {
		values := map[string]dyndbTypes.AttributeValue{
			TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
		}
		setJson, hasSetJson := t.Creates[key]
		_, createStrList := createsStrLists[key]
		if hasSetJson {
			rawJson, err := attributevalue.Marshal(setJson)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
			}
			values[TableJsonField] = rawJson
		}
		if createStrList {
			rawJson, err := attributevalue.Marshal([]string{})
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
			}
			values[TableStrListField] = rawJson
		}
		item := dyndbTypes.TransactWriteItem{
			Put: &dyndbTypes.Put{
				ExpressionAttributeNames: map[string]string{RenameTableKey: TableKey},
				Item:                     values,
				TableName:                aws.String(todo.Table),
				ConditionExpression:      aws.String(ConditionKeyDoesntExist),
			},
		}
		transactions = append(transactions, item)
	}
	for key, value := range t.Overwrites {
		values := map[string]dyndbTypes.AttributeValue{
			TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
		}
		rawJson, err := attributevalue.Marshal(value)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
		}
		values[TableJsonField] = rawJson
		item := dyndbTypes.TransactWriteItem{
			Put: &dyndbTypes.Put{
				TableName: aws.String(todo.Table),
				Item:      values,
			},
		}
		transactions = append(transactions, item)
	}
	for key, fields := range t.SetFields {
		values := map[string]dyndbTypes.AttributeValue{}
		updateExpressions := make([]string, 0)
		for fieldName, fieldValue := range fields {
			rawJson, err := attributevalue.Marshal(fieldValue)
			if err != nil {
				return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
			}
			values[":"+fieldName] = rawJson
			update := TableJsonField + "." + fieldName + " = :" + fieldName
			updateExpressions = append(updateExpressions, update)
		}
		updateExpression := "SET " + strings.Join(updateExpressions, ",")
		item := dyndbTypes.TransactWriteItem{
			Update: &dyndbTypes.Update{
				ExpressionAttributeNames:  map[string]string{RenameTableKey: TableKey},
				ExpressionAttributeValues: values,
				TableName:                 aws.String(todo.Table),
				ConditionExpression:       aws.String(ConditionKeyExists),
				UpdateExpression:          aws.String(updateExpression),
				Key: map[string]dyndbTypes.AttributeValue{
					TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
				},
			},
		}
		fmt.Printf("Update: %#v", updateExpression)
		transactions = append(transactions, item)
	}
	for key, strs := range t.StrListAppends {
		rawJson, err := attributevalue.Marshal(strs)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
		}
		item := dyndbTypes.TransactWriteItem{
			Update: &dyndbTypes.Update{
				ExpressionAttributeNames: map[string]string{RenameTableKey: TableKey},
				ExpressionAttributeValues: map[string]dyndbTypes.AttributeValue{
					RenameNewItems: rawJson,
				},
				Key: map[string]dyndbTypes.AttributeValue{
					TableKey: &dyndbTypes.AttributeValueMemberS{Value: key},
				},
				TableName:           aws.String(todo.Table),
				ConditionExpression: aws.String(ConditionKeyExists),
				UpdateExpression:    aws.String(AppendToStrListExpr),
			},
		}
		transactions = append(transactions, item)
	}

	if len(uuidStr) > RequestTokenSize {
		uuidStr = uuidStr[len(uuidStr)-RequestTokenSize:]
	}
	input := dyndb.TransactWriteItemsInput{
		TransactItems:      transactions,
		ClientRequestToken: aws.String(uuidStr),
	}
	_, err = todo.DB.TransactWriteItems(context.TODO(), &input)
	if err != nil {
		fmt.Printf("Error: %v", err.Error())
		return fiber.NewError(fiber.StatusInternalServerError, errMsg+err.Error())
	}
	return nil
}
