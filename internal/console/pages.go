package console

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

var pageTitles = map[string]string{
	"network":   "管网运行",
	"stations":  "换热站联控",
	"incidents": "事故处置",
	"dispatch":  "热网调度",
	"timeline":  "运维时间线",
}

func Register(router chi.Router) {
	router.Handle("/assets/*", http.StripPrefix("/assets/", staticHandler()))
	for path, title := range pageTitles {
		path, title := path, title
		router.Get("/"+path, func(w http.ResponseWriter, _ *http.Request) {
			servePage(w, path, title)
		})
	}
	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/network", http.StatusTemporaryRedirect)
	})
}

func servePage(w http.ResponseWriter, page, title string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	template := `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>HeatPulse - {{TITLE}}</title><link rel="stylesheet" href="/assets/app.css"></head>
<body data-page="{{PAGE}}"><header><div><strong>HeatPulse</strong><span>城市供热联控</span></div>
<nav><a href="/network">管网</a><a href="/stations">换热站</a><a href="/incidents">事故</a><a href="/dispatch">调度</a><a href="/timeline">时间线</a></nav></header>
<main><section class="heading"><div><p>OPERATIONS / {{PAGE_UPPER}}</p><h1>{{TITLE}}</h1></div><span id="connection">连接中</span></section>
<section id="summary" class="summary" aria-live="polite"></section><section><div class="section-title"><h2>实时状态</h2><button id="refresh" type="button">刷新</button></div><div id="content" class="grid"></div></section>
</main><script src="/assets/app.js"></script></body></html>`
	replacer := strings.NewReplacer("{{TITLE}}", title, "{{PAGE}}", page, "{{PAGE_UPPER}}", strings.ToUpper(page))
	_, _ = w.Write([]byte(replacer.Replace(template)))
}
