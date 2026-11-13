package integrations

import "embed"

//go:embed assets/plugins/*.js
var Plugins embed.FS

//go:embed assets/hermes/plugin.yaml assets/hermes/__init__.py
var Hermes embed.FS
