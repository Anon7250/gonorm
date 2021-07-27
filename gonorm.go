package gonorm

import (
	"github.com/google/uuid"
)

type WriteTransaction struct {
	// Create json items that must not already exist
	Creates map[string]interface{}

	Overwrites map[string]interface{}

	// Sets fields of json items
	SetFields map[string]map[string]interface{}

	// Append strings to lists of strings
	StrListAppends map[string][]string

	// Create empty lists of strings
	StrListCreates []string
}

type KeyValueDB interface {
	HasKey(key string) (bool, error)
	GetJson(key string, valueOut interface{}) error
	GetStringList(key string, valueOut *[]string) error
	DoWriteTransaction(transaction WriteTransaction) error
}

func GetUUID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.URN(), nil
}
