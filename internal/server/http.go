package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// wraps our log httpServer in an http.Server with handlers registered
func NewHTTPServer(addr string) *http.Server {
	httpServer := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpServer.handleProduce).Methods("POST")
	r.HandleFunc("/", httpServer.handleConsume).Methods("Get")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// struct to hold our log and our handler methods
type httpServer struct {
	Log *Log
}

// don't confuse this with http.Server
func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

// unmarshalls request, appeds message to the log, returns offset
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req) // unmarshall
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record) // append to log
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(resp) // return offset
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// unmarshalls request, finds record at given offset, returns record
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req) // unmarshall
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset) // find record
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(record) // return record
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
