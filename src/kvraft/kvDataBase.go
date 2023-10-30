package kvraft

type KvDataBase struct {
	dataBase map[string]string
}

func NewKvDataBase() *KvDataBase {
	return &KvDataBase{
		dataBase: make(map[string]string),
	}
}

func (db *KvDataBase) get(key string) (string, Err) {
	if val, ok := db.dataBase[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (db *KvDataBase) append(key string, val string) Err {
	if _, ok := db.dataBase[key]; ok {
		db.dataBase[key] += val
	} else {
		db.put(key, val)
	}
	return OK
}
func (db *KvDataBase) put(key, val string) Err {
	db.dataBase[key] = val
	return OK
}
