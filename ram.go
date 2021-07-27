package gonorm

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gofiber/fiber/v2"
)

type RAMDB struct {
	Jsons            sync.Map
	StrLists         sync.Map
	TransactionLocks sync.Map
}

func NewRAMDB() *RAMDB {
	return &RAMDB{sync.Map{}, sync.Map{}, sync.Map{}}
}

func (todo *RAMDB) HasKey(key string) (bool, error) {
	todo.lock(key)
	defer todo.unlock(key)

	_, ok1 := todo.Jsons.Load(key)
	_, ok2 := todo.StrLists.Load(key)
	return ok1 || ok2, nil
}

func (todo *RAMDB) GetJson(key string, valueOut interface{}) error {
	todo.lock(key)
	defer todo.unlock(key)

	ans, ok := todo.Jsons.Load(key)
	if !ok {
		return fiber.NewError(fiber.StatusNotFound, "No such DB key: "+key)
	}

	ansBytes, ok := ans.([]byte)
	if !ok {
		return fiber.NewError(
			fiber.StatusInternalServerError,
			fmt.Sprintf("Invalid json for key '%s': %T: %#v", key, ans, ans),
		)
	}
	return json.Unmarshal(ansBytes, valueOut)
}

func (todo *RAMDB) SetJson(key string, value interface{}) error {
	todo.lock(key)
	defer todo.unlock(key)

	ans, err := json.Marshal(value)
	if err != nil {
		return err
	}
	todo.Jsons.Store(key, ans)
	return nil
}

func (todo *RAMDB) GetStringList(key string, valueOut *[]string) error {
	todo.lock(key)
	defer todo.unlock(key)

	rawList, ok := todo.StrLists.Load(key)
	if !ok {
		return fiber.NewError(fiber.StatusNotFound, "No such DB key: "+key)
	}

	ans, ok := rawList.([]string)
	if !ok {
		return fiber.NewError(
			fiber.StatusInternalServerError,
			fmt.Sprintf("Invalid list for key '%s': %T: %#v", key, rawList, rawList),
		)
	}

	*valueOut = ans
	return nil
}

func (todo *RAMDB) DoWriteTransaction(t WriteTransaction) error {
	locked_map := make(map[string]bool)
	for key := range t.Creates {
		_, locked := locked_map[key]
		if locked {
			continue
		}
		todo.lock(key)
		defer todo.unlock(key)
		locked_map[key] = true
	}
	for key := range t.Overwrites {
		_, locked := locked_map[key]
		if locked {
			continue
		}
		todo.lock(key)
		defer todo.unlock(key)
		locked_map[key] = true
	}
	for key := range t.SetFields {
		_, locked := locked_map[key]
		if locked {
			continue
		}
		todo.lock(key)
		defer todo.unlock(key)
		locked_map[key] = true
	}
	for key := range t.StrListAppends {
		_, locked := locked_map[key]
		if locked {
			continue
		}
		todo.lock(key)
		defer todo.unlock(key)
		locked_map[key] = true
	}
	for _, key := range t.StrListCreates {
		_, locked := locked_map[key]
		if locked {
			continue
		}
		todo.lock(key)
		defer todo.unlock(key)
		locked_map[key] = true
	}

	for key := range t.Creates {
		_, currBytesExists := todo.Jsons.Load(key)
		if currBytesExists {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				fmt.Sprintf("Transanction failed: expecting key %v not to exist, but it does", key),
			)
		}
	}
	for _, key := range t.StrListCreates {
		_, currBytesExists := todo.StrLists.Load(key)
		if currBytesExists {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				fmt.Sprintf("Transanction failed: expecting list %v not to exist, but it does", key),
			)
		}
	}

	oldLists := make(map[string][]string)
	for key := range t.StrListAppends {
		list, currBytesExists := todo.StrLists.Load(key)
		if !currBytesExists {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				fmt.Sprintf("Transanction failed: expecting list %v to exist, but it does not", key),
			)
		}
		oldLists[key] = list.([]string)
	}

	for key, obj := range t.Creates {
		rawJson, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		todo.Jsons.Store(key, rawJson)
	}
	for key, obj := range t.Overwrites {
		rawJson, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		todo.Jsons.Store(key, rawJson)
	}
	for key, fields := range t.SetFields {
		rawJson, ok := todo.Jsons.Load(key)
		if !ok {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				fmt.Sprintf("Transanction failed: expecting item %v to exist, but it does not", key),
			)
		}
		var rawMap map[string]interface{}
		err := json.Unmarshal(rawJson.([]byte), &rawMap)
		if err != nil {
			return fiber.NewError(
				fiber.StatusInternalServerError,
				fmt.Sprintf("Transanction failed: expecting item %v to be a map, but it is not", key),
			)
		}
		for fieldName, fieldVal := range fields {
			rawMap[fieldName] = fieldVal
		}
		rawJson, err = json.Marshal(rawMap)
		if err != nil {
			return err
		}
		todo.Jsons.Store(key, rawJson)
	}
	for _, key := range t.StrListCreates {
		var list = make([]string, 0)
		todo.StrLists.Store(key, list)
	}
	for key, appends := range t.StrListAppends {
		list := oldLists[key]
		list = append(list, appends...)
		todo.StrLists.Store(key, list)
	}
	return nil
}

func (todo *RAMDB) lock(key string) {
	mutex, _ := todo.TransactionLocks.LoadOrStore(key, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
}

func (todo *RAMDB) unlock(key string) {
	mutex, ok := todo.TransactionLocks.Load(key)
	if !ok {
		return
	}

	mutex.(*sync.Mutex).Unlock()
}
