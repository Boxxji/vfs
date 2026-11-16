//go:build fuse && linux

package mount

import (
	"embed"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The sources are read from the test binary, so the checks run wherever it does.
//
//go:embed fuse_unix.go errno.go
var sources embed.FS

func parse(t *testing.T, fset *token.FileSet, name string) *ast.File {
	t.Helper()
	src, err := sources.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestNodesAreBuiltOnlyByTheirConstructors: a node, a handle or a session built with a
// struct literal elsewhere would carry no mount context, and the first read of a chunk
// against a real store would panic on it. The source is checked, so a new code path that
// builds one is refused whatever tests exercise it.
func TestNodesAreBuiltOnlyByTheirConstructors(t *testing.T) {
	allowed := map[string]bool{"newSession": true, "node": true, "handle": true}
	fset := token.NewFileSet()
	for _, name := range []string{"fuse_unix.go", "errno.go"} {
		f := parse(t, fset, name)
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if id, ok := lit.Type.(*ast.Ident); ok && (id.Name == "node" || id.Name == "handle" || id.Name == "session") && !allowed[fn.Name.Name] {
					t.Errorf("%s: %s builds a %s with a struct literal; use the constructor", fset.Position(lit.Pos()), fn.Name.Name, id.Name)
				}
				return true
			})
		}
	}
}

// TestEveryHandlerOutlivesItsRequest: no request is answered EINTR, so no handler (an
// exported method the FUSE library calls) may run on its request's context: each that uses
// its ctx first replaces it with one the request cannot end, and what a handler calls is
// passed that context. Exactly three do it with patient, which ends the context three seconds
// after an interrupt: the ones that change nothing and are only read, a lookup, a getattr
// and a readdir. A read, a write, a commit or a close is never one of them.
func TestEveryHandlerOutlivesItsRequest(t *testing.T) {
	patientOK := map[string]bool{"Attr": true, "Lookup": true, "ReadDirAll": true}
	for _, file := range []string{"fuse_unix.go"} {
		fset := token.NewFileSet()
		f := parse(t, fset, file)
		handlers := 0
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() || len(fn.Type.Params.List) == 0 {
				continue
			}
			first := fn.Type.Params.List[0]
			if len(first.Names) == 0 || first.Names[0].Name != "ctx" {
				continue
			}
			handlers++
			detached, patient, uses := false, false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					if len(n.Rhs) == 1 {
						if call, ok := n.Rhs[0].(*ast.CallExpr); ok {
							switch fun := call.Fun.(type) {
							case *ast.SelectorExpr:
								if l, ok := n.Lhs[0].(*ast.Ident); ok && l.Name == "ctx" && fun.Sel.Name == "WithoutCancel" {
									detached = true
								}
							case *ast.Ident:
								if l, ok := n.Lhs[0].(*ast.Ident); ok && l.Name == "ctx" && fun.Name == "patient" {
									patient = true
								}
							}
						}
					}
				case *ast.Ident:
					if n.Name == "ctx" {
						uses = true
					}
				}
				return true
			})
			switch {
			case patient && !patientOK[fn.Name.Name]:
				t.Errorf("%s: %s waits for the store with patient: only Attr, Lookup and ReadDirAll may", fset.Position(fn.Pos()), fn.Name.Name)
			case uses && !detached && !patient:
				t.Errorf("%s: %s uses its request's context; a request is never answered EINTR, so it must run on context.WithoutCancel(ctx)", fset.Position(fn.Pos()), fn.Name.Name)
			case patientOK[fn.Name.Name] && !patient:
				t.Errorf("%s: %s is read-only and waits for the store: it takes patient, so that a killed caller is freed", fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
		if handlers < 10 {
			t.Fatalf("found only %d handlers in %s: the check looks at the wrong thing", handlers, file)
		}
	}
}
