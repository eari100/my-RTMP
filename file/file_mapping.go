package file

import "net/http"

func FileRegisterRoutes() {
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
}
