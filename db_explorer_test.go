package main

import (
	"net/url"
	"reflect"
	"testing"
)

func UrlParsingTest(t *testing.T, url *url.URL, expectedParams *RequestParams, expectedError error) {
	params := &RequestParams{}
	error := params.ParseRequestURL(url)

	if error != nil {
		if !reflect.DeepEqual(error, expectedError) {
			t.Errorf("!reflect.DeepEqual(%v, %v)", error, expectedError)
		}
	} else {
		if !reflect.DeepEqual(params, expectedParams) {
			t.Error("!reflect.DeepEqual(params, expectedParams)")
		}
	}
}

//GET / - возвращает список все таблиц (которые мы можем использовать в дальнейших запросах)
//GET /$table?limit=5&offset=7 - возвращает список из 5 записей (limit) начиная с 7-й (offset) из таблицы $table. limit по-умолчанию 5, offset 0
//GET /$table/$id - возвращает информацию о самой записи или 404
//PUT /$table - создаёт новую запись, данный по записи в теле запроса (POST- параметры)
//POST /$table/$id - обновляет запись, данные приходят в теле запроса (POST- параметры)
//DELETE /$table/$id - удаляет запись

func TestSlash(t *testing.T) {
	u, _ := url.Parse("https://host.com/")
	UrlParsingTest(t, u, &RequestParams{}, nil)
}

func TestTableLimitOffset(t *testing.T) {
	u, _ := url.Parse("https://host.com/$table?limit=5&offset=7")
	UrlParsingTest(t, u, &RequestParams{Table: "$table", Limit: 5, Offset: 7}, nil)
}

func TestTableLimitOffsetBroken(t *testing.T) {
	u, _ := url.Parse("https://host.com/$table?limit=5")
	UrlParsingTest(t, u, &RequestParams{Table: "$table", Limit: 5}, nil)
}

func TestTable(t *testing.T) {
	u, _ := url.Parse("https://host.com/$table")
	UrlParsingTest(t, u, &RequestParams{Table: "$table"}, nil)
}

func TestTableId(t *testing.T) {
	u, _ := url.Parse("https://host.com/$table/15")
	UrlParsingTest(t, u, &RequestParams{Table: "$table", Id: 15}, nil)
}
