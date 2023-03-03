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

func readTypes(db *sql.DB, tableName string) ([]*sql.ColumnType, error) {
	s := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, qe := db.Query(s)

	if qe != nil {
		return nil, qe
	}
	colTypes, cte := rows.ColumnTypes()

	if cte != nil {
		return nil, cte
	}
	var columnTypes []*sql.ColumnType
	for _, ct := range colTypes {
		columnTypes = append(columnTypes, ct)
	}
	cle := rows.Close()

	if cle != nil {
		return nil, cle
	}

	return columnTypes, nil
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	tableColumns := map[string][]*sql.ColumnType{}
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
			nullable, _ := ct.Nullable()

			fmt.Printf("[%v][%v][%v][%v]\n", t, ct.Name(), ct.DatabaseTypeName(), nullable)
		}
	}

	return &DbExplorer{
		db:          db,
		columnTypes: tableColumns,
	}, nil
}

type DbExplorer struct {
	db          *sql.DB
	columnTypes map[string][]*sql.ColumnType
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
	b, _ := json.Marshal(sr)

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

func rowsToJson(rows *sql.Rows) ([]interface{}, error) {
	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, err
	}

	count := len(columnTypes)
	finalRows := make([]interface{}, 0, 10)

	for rows.Next() {
		scanArgs := make([]interface{}, count)

		// заполняем scanArgs указателями на соответсвующий тип
		for i, v := range columnTypes {
			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
				break
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
				break
			case "INT4":
				scanArgs[i] = new(sql.NullInt64)
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
		for i, v := range columnTypes {
			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[v.Name()] = z.Bool
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[v.Name()] = z.Int32
				continue
			}

			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	return finalRows, nil
}

//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
func (explorer *DbExplorer) handleGetShowAllTables(w http.ResponseWriter, r *http.Request) {
	var keys []string
	for k, _ := range explorer.columnTypes {
		keys = append(keys, k)
	}
	handleServerResponse(w, keys)
}

//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
func (explorer *DbExplorer) handleGetTableEntities(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	rows, qe := explorer.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", rp.Table, rp.Limit, rp.Offset))
	panicOnError(qe)
	js, je := rowsToJson(rows)
	panicOnError(je)
	err := rows.Close()
	panicOnError(err)
	handleServerResponse(w, js)
}

//GET /$table/$id - возвращает информацию о самой записи или 404
func (explorer *DbExplorer) handleGetTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	handleServerResponse(w, rp)
}

//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
func (explorer *DbExplorer) handlePutTableEntity(w http.ResponseWriter, r *http.Request) {
	rp := &RequestParams{}
	panicOnError(rp.ParseRequestURL(r.URL))
	handleServerResponse(w, rp)
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
	handleServerResponse(w, rp)
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
