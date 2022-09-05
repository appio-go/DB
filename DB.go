package DB

import (
	"database/sql"
	"errors"
	"fmt"
	dm "github.com/appio-go/driver-mysql"
	"reflect"
	"strconv"
	"strings"
)

type DB struct{}
type Scanner interface {
	Scan(src any) error
}

func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}
func asString(src any) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

//New - adds new row to DB, counts ID as primary index
func (m *DB) New(tblName string, model any) (int64, error) {
	dstModel := reflect.ValueOf(model)

	db, err := dm.MySQLConnection()

	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()

	var dstNames []string
	var dstNamesQ []string
	var dstValuesAny []any

	for i := 0; i < dstModel.NumField(); i++ {
		json := dstModel.Type().Field(i).Tag.Get("json")
		if json != "" && json != "id" {
			dstNames = append(dstNames, json)
			dstNamesQ = append(dstNamesQ, "?")
			dstValuesAny = append(dstValuesAny, dstModel.Field(i).Interface())
		}
	}

	fields := strings.Join(dstNames, "`,`")
	qs := strings.Join(dstNamesQ, ",")

	query := "INSERT INTO " + tblName + "(`" + fields + "`) VALUES (" + qs + ")"

	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}

	res, err := stmt.Exec(dstValuesAny...)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil

}

//Insert - exec query on DB and return new row id
func (m *DB) Insert(query string, args ...any) (int64, error) {
	db, err := dm.MySQLConnection()

	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()

	stmt, err := db.Prepare(query)
	if err != nil {
		return 0, err
	}

	defer func() { _ = stmt.Close() }()

	res, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil

}

//Exec - just exec query
func (m *DB) Exec(query string, args ...any) error {
	db, err := dm.MySQLConnection()

	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}

	defer func() { _ = stmt.Close() }()

	_, err = stmt.Exec(args...)

	return err

}

//QueryRows - runs query in MySQL database return *sql.Rows or error if something went wrong !!! Achtung !!! Don't forget to call rows.Close()
func (m *DB) QueryRows(query string, args ...any) (*sql.Rows, error) {
	db, err := dm.MySQLConnection()

	if err != nil {
		return nil, err
	}

	defer func() { _ = db.Close() }()

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

//QueryRow - query for single row from DB and parse it model (don't add & to model value, as it's already pointer!!!), returns error or nil if all ok
func (m *DB) QueryRow(query string, model any, args ...any) error {
	db, err := dm.MySQLConnection()

	// language=SQL
	sqlQuery := query

	if err != nil {
		return err
	}

	defer func() { _ = db.Close() }()

	rows, err := db.Query(sqlQuery, args...)
	if err != nil {
		return err
	}

	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return m.ScanRows(rows, model)
	}

	return sql.ErrNoRows
}

func (m *DB) ScanRows(rows *sql.Rows, model any) error {
	dstModel := reflect.ValueOf(model)
	if dstModel.Kind() == reflect.Ptr {
		//fmt.Println("is pointer")
		dstModel = dstModel.Elem()
	} else {
		return errors.New("not a pointer")
	}
	var count int

	dstNames := make(map[string]int)

	for i := 0; i < dstModel.NumField(); i++ {
		json := dstModel.Type().Field(i).Tag.Get("json")
		if json != "" {
			count++
			dstNames[json] = i
		}
	}

	values := make([]interface{}, count)
	scanArgs := make([]interface{}, count)

	for i := range values {
		scanArgs[i] = &values[i]
	}

	columns, err := rows.Columns()
	if err != nil {
		//fmt.Println("rows.Columns()", err.Error())
		return err
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		return err
	}

	for i, v := range values {
		if dstIndex, ok := dstNames[columns[i]]; ok {
			//fmt.Println(dstNames[columns[i]], dstModel.Field(dstIndex).Type().String())
			switch dstModel.Field(dstIndex).Kind() {
			case reflect.Pointer:
				if v == nil {
					dstModel.Field(dstIndex).Set(reflect.Zero(dstModel.Field(dstIndex).Type()))
					continue
				}
				dstModel.Field(dstIndex).Set(reflect.New(dstModel.Field(dstIndex).Type().Elem()))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if v == nil {
					return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Kind())
				}
				s := asString(v)
				i64, err := strconv.ParseInt(s, 10, dstModel.Field(dstIndex).Type().Bits())
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Kind(), err.Error())
				}
				dstModel.Field(dstIndex).SetInt(i64)
				continue
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if v == nil {
					return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Kind())
				}
				s := asString(v)
				u64, err := strconv.ParseUint(s, 10, dstModel.Field(dstIndex).Type().Bits())
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Kind(), err)
				}
				dstModel.Field(dstIndex).SetUint(u64)
				continue
			case reflect.Float32, reflect.Float64:
				if v == nil {
					return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Kind())
				}
				s := asString(v)
				f64, err := strconv.ParseFloat(s, dstModel.Field(dstIndex).Type().Bits())
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Kind(), err.Error())
				}
				dstModel.Field(dstIndex).SetFloat(f64)
				continue
			case reflect.Bool:
				if v == nil {
					return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Kind())
				}
				s := asString(v)
				b, err := strconv.ParseBool(s)
				if err != nil {
					return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Kind(), err.Error())
				}
				dstModel.Field(dstIndex).SetBool(b)
				continue
			case reflect.String:
				if v == nil {
					return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Kind())
				}
				switch v := v.(type) {
				case string:
					dstModel.Field(dstIndex).SetString(v)
					continue
				case []byte:
					dstModel.Field(dstIndex).SetString(string(v))
					continue
				}
			}
			sv := reflect.ValueOf(v)

			if dstModel.Field(dstIndex).CanInterface() {
				switch dstModel.Field(dstIndex).Field(0).Kind() {
				case reflect.Pointer:
					if v == nil {
						dstModel.Field(dstIndex).Field(0).Set(reflect.Zero(dstModel.Field(dstIndex).Field(0).Type()))
						dstModel.Field(dstIndex).Field(1).SetBool(true)
						continue
					}
					dstModel.Field(dstIndex).Field(0).Set(reflect.New(dstModel.Field(dstIndex).Field(0).Type().Elem()))
					dstModel.Field(dstIndex).Field(1).SetBool(true)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					if v == nil {
						return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Field(0).Kind())
					}
					s := asString(v)
					i64, err := strconv.ParseInt(s, 10, dstModel.Field(dstIndex).Field(0).Type().Bits())
					if err != nil {
						return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Field(0).Kind(), err.Error())
					}
					//fmt.Println("-->", i64)
					dstModel.Field(dstIndex).Field(0).SetInt(i64)
					dstModel.Field(dstIndex).Field(1).SetBool(true)
					continue
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					if v == nil {
						return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Field(0).Kind())
					}
					s := asString(v)
					u64, err := strconv.ParseUint(s, 10, dstModel.Field(dstIndex).Field(0).Type().Bits())
					if err != nil {
						return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Field(0).Kind(), err)
					}
					dstModel.Field(dstIndex).Field(0).SetUint(u64)
					dstModel.Field(dstIndex).Field(1).SetBool(true)
					continue
				case reflect.Float32, reflect.Float64:
					if v == nil {
						return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Field(0).Kind())
					}
					s := asString(v)
					f64, err := strconv.ParseFloat(s, dstModel.Field(dstIndex).Field(0).Type().Bits())
					if err != nil {
						return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Field(0).Kind(), err.Error())
					}
					dstModel.Field(dstIndex).Field(0).SetFloat(f64)
					dstModel.Field(dstIndex).Field(1).SetBool(true)
					continue
				case reflect.Bool:
					if v == nil {
						return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Field(0).Kind())
					}
					s := asString(v)
					b, err := strconv.ParseBool(s)
					if err != nil {
						return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", v, s, dstModel.Field(dstIndex).Field(0).Kind(), err.Error())
					}
					dstModel.Field(dstIndex).Field(0).SetBool(b)
					dstModel.Field(dstIndex).Field(1).SetBool(true)
					continue
				case reflect.String:
					if v == nil {
						return fmt.Errorf("converting NULL to %s is unsupported", dstModel.Field(dstIndex).Field(0).Kind())
					}
					switch v := v.(type) {
					case string:
						dstModel.Field(dstIndex).Field(0).SetString(v)
						dstModel.Field(dstIndex).Field(1).SetBool(true)
						continue
					case []byte:
						dstModel.Field(dstIndex).Field(0).SetString(string(v))
						dstModel.Field(dstIndex).Field(1).SetBool(true)
						continue
					}
				}
			}

			if dstModel.Field(dstIndex).Kind() == sv.Kind() && sv.Type().ConvertibleTo(dstModel.Field(dstIndex).Type()) {
				dstModel.Field(dstIndex).Set(dstModel.Field(dstIndex).Convert(dstModel.Field(dstIndex).Type()))
				continue
			}

			if sv.IsValid() && sv.Type().AssignableTo(dstModel.Field(dstIndex).Type()) {
				switch b := v.(type) {
				case []byte:
					dstModel.Field(dstIndex).Set(reflect.ValueOf(cloneBytes(b)))
				default:
					dstModel.Field(dstIndex).Set(sv)
				}
				continue
			}

		}
	}
	return nil
}
