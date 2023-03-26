package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func maps(in []string, fn func(string) string) []string {
	ret := make([]string, len(in))
	for i, v := range in {
		ret[i] = fn(v)
	}

	return ret
}

func keys(m map[string]Any) []string {
	var keys []string
	for k, _ := range m {
		keys = append(keys, k)
	}

	return keys
}

func readTables(db *sql.DB) ([]string, error) {
	rows, qe := db.Query("SHOW TABLES")

	if qe != nil {
		return nil, qe
	}

	var tables []string
	for rows.Next() {
		tableName := ""
		se := rows.Scan(&tableName)

		if se != nil {
			return nil, se
		}

		tables = append(tables, tableName)
	}
	ce := rows.Close()

	if ce != nil {
		return nil, ce
	}

	return tables, nil
}

type ColumnInfo struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}

type Any = interface{}

func (receiver *ColumnInfo) ParseFullColumn(scanArgs []Any) error {
	// Field(0) Type(1) Null(3) Key(4)
	receiver.Name = *(scanArgs[0].(*string))
	receiver.Type = *(scanArgs[1].(*string))
	receiver.Nullable = *(scanArgs[3].(*string)) == "YES"
	receiver.PrimaryKey = *(scanArgs[4].(*string)) == "PRI"

	return nil
}

func (receiver *ColumnInfo) ParseFormValue(form url.Values) (Any, error) {
	name := receiver.Name
	nullable := receiver.Nullable
	tp := receiver.Type
	pk := receiver.PrimaryKey
	fhas := form.Has(name)
	fsval := form.Get(name)

	if fhas {
		if pk {
			return nil, nil
		} else {
			switch tp {
			case "int":
				return strconv.Atoi(fsval)
			case "varchar(255)":
				fallthrough
			case "text":
				return fsval, nil
			default:
				return fsval, nil
			}
		}
	} else {
		if nullable {
			return nil, nil
		} else {
			return nil, fmt.Errorf("%s is nil", name)
		}
	}
}

func readTypes(db *sql.DB, tableName string) ([]ColumnInfo, error) {
	s := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", tableName)
	rows, qe := db.Query(s)

	if qe != nil {
		return nil, qe
	}

	var columns []ColumnInfo
	for rows.Next() {
		column := ColumnInfo{}
		scanArgs := make([]interface{}, 9)

		scanArgs[0] = new(string)
		scanArgs[1] = new(string)
		scanArgs[2] = new(Any)
		scanArgs[3] = new(string)
		scanArgs[4] = new(string)
		scanArgs[5] = new(Any)
		scanArgs[6] = new(Any)
		scanArgs[7] = new(Any)
		scanArgs[8] = new(Any)

		se := rows.Scan(scanArgs...)

		if se != nil {
			return nil, se
		}

		pe := column.ParseFullColumn(scanArgs)

		if pe != nil {
			return nil, pe
		}

		columns = append(columns, column)
	}
	ce := rows.Close()

	if ce != nil {
		return nil, ce
	}

	return columns, nil
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	tableColumns := map[string][]ColumnInfo{}
	tables, e := readTables(db)

	fmt.Printf("tables: %v\n", tables)

	if e != nil {
		return nil, e
	}

	for _, t := range tables {
		columnTypes, e := readTypes(db, t)

		if e != nil {
			return nil, e
		}

		tableColumns[t] = columnTypes
	}

	for t, v := range tableColumns {
		for _, ct := range v {
			fmt.Printf("[%v][%v]=%v\n", t, ct.Name, ct)
		}
	}

	return &DbExplorer{
		db:          db,
		columnTypes: tableColumns,
	}, nil
}

type DbExplorer struct {
	db          *sql.DB
	columnTypes map[string][]ColumnInfo
}

type ApiError struct {
	HTTPStatus int
	Err        error
}

func (ae ApiError) Error() string {
	return ae.Err.Error()
}

type ServerResponse struct {
	Error    string      `json:"error"`
	Response interface{} `json:"response,omitempty"`
}

func (sr ServerResponse) Marshal() []byte {
	b, _ := json.MarshalIndent(sr, "", "  ")

	return b
}

func handleServerError(w http.ResponseWriter, httpStatus int, err error) {
	w.WriteHeader(httpStatus)
	w.Write(ServerResponse{
		Error: ApiError{
			httpStatus,
			err,
		}.Error(),
	}.Marshal())
}

func handleServerResponse(w http.ResponseWriter, response interface{}) {
	w.Write(ServerResponse{
		Error:    "",
		Response: response,
	}.Marshal())
}

func errorMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("errorMiddleware", r.URL.Path)
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("recovered", err)

				e := fmt.Errorf("%s", err)
				handleServerError(w, http.StatusInternalServerError, e)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func panicOnError(e error) {
	if e != nil {
		panic(e)
	}
}

type RequestParams struct {
	Table  string
	Limit  int
	Offset int
	Id     int
}

func isRoot(url *url.URL) bool { return url.Path == "/" }

func isOneSlashLong(url *url.URL) bool {
	return len(url.Path) > 1 && strings.Count(url.Path, "/") == 1
}

func isTwoSlashLong(url *url.URL) bool {
	return len(url.Path) > 1 && strings.Count(url.Path, "/") == 2
}

func (receiver *RequestParams) ParseRequestURL(url *url.URL) error {
	noPrefixPath := strings.TrimPrefix(url.Path, "/")

	if isOneSlashLong(url) {
		receiver.Table = noPrefixPath
		q := url.Query()
		ls := q.Get("limit")
		os := q.Get("offset")
		l, e := strconv.Atoi(ls)
		if len(ls) > 0 && e == nil {
			receiver.Limit = l
		}
		o, e := strconv.Atoi(os)
		if len(ls) > 0 && e == nil {
			receiver.Offset = o
		}

	} else if isTwoSlashLong(url) {
		split := strings.Split(noPrefixPath, "/")
		receiver.Table = split[0]
		ids := split[1]
		id, e := strconv.Atoi(ids)
		if len(ids) > 0 && e == nil {
			receiver.Id = id
		}
	}

	return nil
}

func rowsToJson(infos []ColumnInfo, rows *sql.Rows) ([]interface{}, error) {
	count := len(infos)
	finalRows := make([]interface{}, 0, 10)

	for rows.Next() {
		scanArgs := make([]interface{}, count)

		// заполняем scanArgs указателями на соответсвующий тип
		for i, v := range infos {
			switch v.Type {
			case "int":
				scanArgs[i] = new(sql.NullInt64)
				break
			case "varchar(255)":
				fallthrough
			case "text":
				scanArgs[i] = new(sql.NullString)
				break
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return nil, err
		}

		masterData := map[string]interface{}{}

		// на основе scanArgs раскладываем в мапу masterData правильные значения
		for i, v := range infos {
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name] = z.String
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name] = z.Int64
				continue
			}

			masterData[v.Name] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	return finalRows, nil
}

func (explorer *DbExplorer) findPK(tableName string) (string, error) {
	columns := explorer.columnTypes[tableName]

	if columns != nil {
		for _, col := range columns {
			if col.PrimaryKey {
				return col.Name, nil
			}
		}
	}

	return "", fmt.Errorf("cannot find pk")
}

func anyToSQLValue(any Any) (string, error) {
	if i, iok := any.(int); iok {
		return fmt.Sprintf("%d", i), nil
	} else if s, sok := any.(string); sok {
		return fmt.Sprintf("'%s'", s), nil
	}

	return "", fmt.Errorf("anyToSQLValue: no such type")
}

//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
func (explorer *DbExplorer) handleGetShowAllTables(w http.ResponseWriter, r *http.Request) {
	var keys []string
	for k, _ := range explorer.columnTypes {
		keys = append(keys, k)
	}
	handleServerResponse(w, map[string]interface{}{
		"tables": keys,
	})
}

//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
func (explorer *DbExplorer) handleGetTableEntities(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	rows, qe := explorer.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", rp.Table, rp.Limit, rp.Offset))
	panicOnError(qe)
	js, je := rowsToJson(explorer.columnTypes[rp.Table], rows)
	panicOnError(je)
	panicOnError(rows.Close())
	handleServerResponse(w, map[string]interface{}{
		"records": js,
	})
}

//GET /$table/$id - возвращает информацию о самой записи или 404
func (explorer *DbExplorer) handleGetTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	pk, e := explorer.findPK(rp.Table)
	panicOnError(e)
	rows, qe := explorer.db.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s='%d'", rp.Table, pk, rp.Id))
	panicOnError(qe)
	js, je := rowsToJson(explorer.columnTypes[rp.Table], rows)
	panicOnError(je)
	panicOnError(rows.Close())

	if len(js) > 0 {
		record := js[0]
		handleServerResponse(w, map[string]interface{}{
			"record": record,
		})
	} else {
		handleServerError(w, http.StatusNotFound, fmt.Errorf("not found"))
	}
}

//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
func (explorer *DbExplorer) handlePutTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	panicOnError(r.ParseForm())

	columnInfo := explorer.columnTypes[rp.Table]
	kv := make(map[string]Any, 5)
	pk, pke := explorer.findPK(rp.Table)
	panicOnError(pke)

	for _, v := range columnInfo {
		if v.PrimaryKey {
			continue
		}

		name := v.Name
		val, pe := v.ParseFormValue(r.Form)
		fmt.Printf("[ParseFormValue]\n")
		fmt.Printf("%v: %v\n", name, val)

		panicOnError(pe)

		if val != nil {
			kv[name] = val
		}
	}

	ks := keys(kv)
	fmt.Printf("[kv]: %v\n", kv)
	fmt.Printf("[ks]: %v\n", ks)
	values := maps(ks, func(k string) string {
		s, e := anyToSQLValue(kv[k])
		panicOnError(e)

		return s
	})
	//ks = maps(ks, func(s string) string { return fmt.Sprintf("`%s`", s) })
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", rp.Table, strings.Join(ks, ", "), strings.Join(values, ", "))
	fmt.Println(insert)
	result, ee := explorer.db.Exec(insert)
	panicOnError(ee)
	lastInsertedId, ie := result.LastInsertId()
	panicOnError(ie)
	handleServerResponse(w, map[string]interface{}{pk: lastInsertedId})
}

//POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST- параметры)
func (explorer *DbExplorer) handlePostTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	handleServerResponse(w, rp)
}

//DELETE /$table/$id - удаляет запись
func (explorer *DbExplorer) handleDeleteTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	result, ee := explorer.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id='%d'", rp.Table, rp.Id))
	panicOnError(ee)
	affected, ae := result.RowsAffected()
	panicOnError(ae)
	handleServerResponse(w, map[string]interface{}{
		"deleted": affected,
	})
}

func (explorer *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
	//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
	//GET /$table/$id - возвращает информацию о самой записи или 404
	//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
	//POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST- параметры)
	//DELETE /$table/$id - удаляет запись

	switch r.Method {
	case "GET":
		if isRoot(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handleGetShowAllTables)).ServeHTTP(w, r)
		} else if isOneSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handleGetTableEntities)).ServeHTTP(w, r)
		} else if isTwoSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handleGetTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

		}
		return
	case "POST":
		if isTwoSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handlePostTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

		}
		return
	case "PUT":
		if isOneSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handlePutTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

		}
		return
	case "DELETE":
		if isTwoSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handleDeleteTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

		}
		return
	}

	handleServerError(w, http.StatusNotFound, fmt.Errorf("unknown method"))
}
