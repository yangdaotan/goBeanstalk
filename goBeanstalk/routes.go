// the simple http route support post get head etc..

package goBeanstalk

import (
	"net/http"
	// "sync"
	// "fmt"
)

const (
	CONNECT = "CONNECT"
	DELETE  = "DELETE"
	GET     = "GET"
	HEAD    = "HEAD"
	OPTIONS = "OPTIONS"
	PATCH   = "PATCH"
	POST    = "POST"
	PUT     = "PUT"
	TRACE   = "TRACE"
)

//route
type route struct {
	method  string
	path    string
	handler http.HandlerFunc
}

type RouteMux struct {
	routes []*route
}

// var instance *RouteMux
// var once sync.Once

func New() *RouteMux {
	return &RouteMux{}
}

// func Ins() *RouteMux {
//     once.Do(func() {
//         instance = &RouteMux{
//     })
//     return instance
// }

// Get adds a new Route for GET requests.
func (m *RouteMux) Get(path string, handler http.HandlerFunc) {
	m.AddRoute(GET, path, handler)
}

// Put adds a new Route for PUT requests.
func (m *RouteMux) Put(path string, handler http.HandlerFunc) {
	m.AddRoute(PUT, path, handler)
}

// Del adds a new Route for DELETE requests.
func (m *RouteMux) Del(path string, handler http.HandlerFunc) {
	m.AddRoute(DELETE, path, handler)
}

// Patch adds a new Route for PATCH requests.
func (m *RouteMux) Patch(path string, handler http.HandlerFunc) {
	m.AddRoute(PATCH, path, handler)
}

// Post adds a new Route for POST requests.
func (m *RouteMux) Post(path string, handler http.HandlerFunc) {
	m.AddRoute(POST, path, handler)
}

func (m *RouteMux) AddRoute(method string, path string, handler http.HandlerFunc) {
	route := &route{method, path, handler}
	m.routes = append(m.routes, route)
}

func (m *RouteMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestPath := r.URL.Path
	// fmt.Println(requestPath)
	// fmt.Println(r.Method)

	for _, route := range m.routes {
		// fmt.Println(route.method)
		// fmt.Println(route.path)
		if r.Method != route.method {
			continue
		}
		if requestPath != route.path {
			continue
		}
		route.handler(w, r)
	}
	http.NotFound(w, r)
	return
}
