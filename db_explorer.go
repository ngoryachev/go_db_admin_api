package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	return &DbExplorer{
		db: db,
	}, nil
}

type DbExplorer struct {
	db *sql.DB
}

// FIXME
func handleServerError(w http.ResponseWriter, httpStatus int, err error) {
	w.WriteHeader(httpStatus)
	//w.Write(ServerResponse{
	//	Error: mapError(ApiError{
	//		httpStatus,
	//		err,
	//	}.Error()),
	//}.Marshal())
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

//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
func (explorer *DbExplorer) handleGetShowAllTables(w http.ResponseWriter, r *http.Request) {

}

//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
func (explorer *DbExplorer) handleGetTableEntities(w http.ResponseWriter, r *http.Request) {

}

//GET /$table/$id - возвращает информацию о самой записи или 404
func (explorer *DbExplorer) handleGetTableEntity(w http.ResponseWriter, r *http.Request) {

}

//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
func (explorer *DbExplorer) handlePutTableEntity(w http.ResponseWriter, r *http.Request) {

}

//POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST- параметры)
func (explorer *DbExplorer) handlePostTableEntity(w http.ResponseWriter, r *http.Request) {

}

//DELETE /$table/$id - удаляет запись
func (explorer *DbExplorer) handleDeleteTableEntity(w http.ResponseWriter, r *http.Request) {

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
		if len(ls) > 0 && e != nil {
			return e
		}
		receiver.Limit = l
		o, e := strconv.Atoi(os)
		if len(ls) > 0 && e != nil {
			return e
		}
		receiver.Offset = o
	} else if isTwoSlashLong(url) {
		split := strings.Split(noPrefixPath, "/")
		receiver.Table = split[0]
		ids := split[1]
		id, e := strconv.Atoi(ids)
		if e != nil {
			return e
		}
		receiver.Id = id
	}

	return nil
}

func (explorer *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
	//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
	//GET /$table/$id - возвращает информацию о самой записи или 404
	//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
	//POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST- параметры)
	//DELETE /$table/$id -  удаляет запись

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

			return
		}
	case "POST":
		if isTwoSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handlePostTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

			return
		}
	case "PUT":
		if isOneSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handlePutTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

			return
		}
	case "DELETE":
		if isTwoSlashLong(r.URL) {
			errorMiddleware(http.HandlerFunc(explorer.handleDeleteTableEntity)).ServeHTTP(w, r)
		} else {
			handleServerError(w, http.StatusNotAcceptable, fmt.Errorf("bad method"))

			return
		}
	}

	handleServerError(w, http.StatusNotFound, fmt.Errorf("unknown method"))
}
