// main.go
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
    "github.com/Anshu-rai89/key-value-store/keyvaluestore"
)

func HandleSet(kv *keyvaluestore.KeyValueStore) http.HandlerFunc {
 return func(w http.ResponseWriter, r *http.Request) {
  var req struct {
   Key   string `json:"key"`
   Value string `json:"value"`
  }

  err := json.NewDecoder(r.Body).Decode(&req)
  if err != nil {
   http.Error(w, "Invalid request body", http.StatusBadRequest)
   return
  }

  kv.Set(req.Key, req.Value)
  w.WriteHeader(http.StatusOK)
 }
}

func HandleGet(kv *keyvaluestore.KeyValueStore) http.HandlerFunc {
 return func(w http.ResponseWriter, r *http.Request) {
  key := r.URL.Query().Get("key")
  if key == "" {
   http.Error(w, "Key parameter is missing", http.StatusBadRequest)
   return
  }

  val, ok := kv.Get(key)
  if !ok {
   http.Error(w, "Key not found", http.StatusNotFound)
   return
  }

  resp := struct {
   Key   string `json:"key"`
   Value string `json:"value"`
  }{Key: key, Value: val}

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(resp)
 }
}

func main() {
 kv := keyvaluestore.NewKeyValueStore()
 http.HandleFunc("/set", HandleSet(kv))
 http.HandleFunc("/get", HandleGet(kv))
 port := 8080
 addr := fmt.Sprintf(":%d", port)
 fmt.Printf("Starting key-value store on http://localhost%s\n", addr)
 err := http.ListenAndServe(addr, nil)
 if err != nil {
  fmt.Printf("Error: %s\n", err)
 }
}