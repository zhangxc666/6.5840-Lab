package kvraft

import "sort"

type KvDataBase struct {
	DataBase map[string]string
}

func NewKvDataBase() *KvDataBase {
	return &KvDataBase{
		DataBase: make(map[string]string),
	}
}

func (db *KvDataBase) get(key string) (string, Err) {
	if val, ok := db.DataBase[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (db *KvDataBase) append(key string, val string) Err {
	if _, ok := db.DataBase[key]; ok {
		db.DataBase[key] += val
	} else {
		db.put(key, val)
	}

	return OK
}
func (db *KvDataBase) put(key, val string) Err {
	db.DataBase[key] = val
	return OK
}

func (db *KvDataBase) String() string {
	s := " map:{\n"
	keys := []string{}
	for k, _ := range db.DataBase {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] < keys[j] {
			return true
		} else {
			return false
		}
	})
	for _, k := range keys {
		v := db.DataBase[k]
		s += "  " + k + ":" + v
		s += "\n"
	}
	s += "}"
	return s
}
