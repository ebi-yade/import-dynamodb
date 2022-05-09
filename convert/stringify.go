package convert

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
)

func Stringify(from events.DynamoDBAttributeValue) (string, error) {
	switch from.DataType() {
	case events.DataTypeNull:
		return "", nil

	case events.DataTypeBoolean:
		return strconv.FormatBool(from.Boolean()), nil

	case events.DataTypeBinary:
		return string(from.Binary()), nil

	case events.DataTypeNumber:
		return from.Number(), nil

	case events.DataTypeString:
		return from.String(), nil

	default:
		return "", fmt.Errorf("convert.Stringify() supports only scalar types that defined in the DynamoDB document.\nfor more, see: %s", dataTypeDocumentURL)
	}
}

const dataTypeDocumentURL = "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes"
