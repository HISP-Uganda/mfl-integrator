package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// FlexString is an int that can be unmarshalled from a JSON field
// that has either a number or a string value.
// E.g. if the json field contains an string "42", the
// FlexInt value will be "42".
type FlexString string

func (fs *FlexString) UnmarshalJSON(data []byte) error {
	var rawValue interface{}
	if err := json.Unmarshal(data, &rawValue); err != nil {
		return err
	}

	switch value := rawValue.(type) {
	case string:
		*fs = FlexString(value)
	case float64:
		*fs = FlexString(strconv.FormatFloat(value, 'f', -1, 64))
	default:
		return fmt.Errorf("unsupported value type: %T", value)
	}

	return nil
}
