package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func (s *httpServer) readLog(w http.ResponseWriter, r *http.Request) {
	var req ReadRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ReadResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) writeLog(w http.ResponseWriter, r *http.Request) {
	var req WriteRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := WriteResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type ReadRequest struct {
	Offset uint64 `json:"offset"`
}
type ReadResponse struct {
	Record Record `json:"record"`
}
type WriteRequest struct {
	Record Record `json:"record"`
}
type WriteResponse struct {
	Offset uint64 `json:"offset"`
}

func NewHTTPServer(addr string) *http.Server {
	s := newHTTPServer()
	r := mux.NewRouter()

	r.HandleFunc("/", s.readLog).Methods("GET")
	r.HandleFunc("/", s.writeLog).Methods("POST")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}
