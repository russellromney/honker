// database/sql and GORM, as guides/orm/go.mdx shows them.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	honker "github.com/russellromney/honker-go"
	sqlite3 "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type expect struct {
	Kind string   `json:"kind"`
	N    *float64 `json:"n"`
	S    string   `json:"s"`
	Ref  string   `json:"ref"`
}

type step struct {
	ID     string  `json:"id"`
	SQL    string  `json:"sql"`
	Args   []any   `json:"args"`
	Store  string  `json:"store"`
	Expect *expect `json:"expect"`
}

type catalog struct {
	Steps []step `json:"steps"`
}

func init() {
	extPath, err := honker.ExtensionPath()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sql.Register("sqlite3_honker", &sqlite3.SQLiteDriver{
		Extensions: []string{extPath},
	})
}

func catalogPath() string {
	if p := os.Getenv("HONKER_ORM_SURFACE"); p != "" {
		return p
	}
	for _, p := range []string{"surface.json", "scripts/proof/orm/surface.json"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	fmt.Fprintln(os.Stderr, "surface.json not found; set HONKER_ORM_SURFACE")
	os.Exit(1)
	return ""
}

func asInt(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case float64:
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("expected int, got %T %v", v, v)
	}
}

func asText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func resolve(token any, prefix string, vars map[string]any) (any, error) {
	s, ok := token.(string)
	if !ok {
		return token, nil
	}
	if strings.HasPrefix(s, "$ns:") {
		return prefix + "_" + s[4:], nil
	}
	if strings.HasPrefix(s, "$json:") {
		keys := strings.Split(s[6:], ",")
		ids := make([]int64, len(keys))
		for i, k := range keys {
			n, err := asInt(vars[k])
			if err != nil {
				return nil, err
			}
			ids[i] = n
		}
		b, err := json.Marshal(ids)
		return string(b), err
	}
	if strings.HasPrefix(s, "$") {
		return vars[s[1:]], nil
	}
	return s, nil
}

func resolveText(text, prefix string, vars map[string]any) string {
	out := text
	for k, v := range vars {
		out = strings.ReplaceAll(out, "$"+k, asText(v))
	}
	return strings.ReplaceAll(out, "$ns:", prefix+"_")
}

func check(exp expect, result any, prefix string, vars map[string]any) error {
	switch exp.Kind {
	case "int_gt":
		n, err := asInt(result)
		if err != nil || n <= int64(*exp.N) {
			return fmt.Errorf("got %v", result)
		}
	case "int_eq":
		n, err := asInt(result)
		if err != nil || n != int64(*exp.N) {
			return fmt.Errorf("got %v", result)
		}
	case "int_ge":
		n, err := asInt(result)
		if err != nil || n < int64(*exp.N) {
			return fmt.Errorf("got %v", result)
		}
	case "int_gt_ref":
		n, err := asInt(result)
		ref, rerr := asInt(vars[exp.Ref])
		if err != nil || rerr != nil || n <= ref {
			return fmt.Errorf("got %v", result)
		}
	case "eq_ref":
		n, err := asInt(result)
		ref, rerr := asInt(vars[exp.Ref])
		if err != nil || rerr != nil || n != ref {
			return fmt.Errorf("got %v", result)
		}
	case "json_len":
		var parsed []any
		if err := json.Unmarshal([]byte(asText(result)), &parsed); err != nil {
			return err
		}
		if len(parsed) != int(*exp.N) {
			return fmt.Errorf("got %v", result)
		}
	case "json_id_eq_ref":
		var parsed []map[string]any
		if err := json.Unmarshal([]byte(asText(result)), &parsed); err != nil {
			return err
		}
		ref, err := asInt(vars[exp.Ref])
		if err != nil {
			return err
		}
		if len(parsed) != 1 {
			return fmt.Errorf("got %v", result)
		}
		id, err := asInt(parsed[0]["id"])
		if err != nil || id != ref {
			return fmt.Errorf("got %v", result)
		}
	case "contains":
		needle := resolveText(exp.S, prefix, vars)
		if !strings.Contains(asText(result), needle) {
			return fmt.Errorf("%q not in %v", needle, result)
		}
	case "empty_string":
		if asText(result) != "" {
			return fmt.Errorf("expected empty string, got %v", result)
		}
	case "is_null":
		if result != nil {
			return fmt.Errorf("expected NULL, got %v", result)
		}
	default:
		return fmt.Errorf("unknown expect kind %s", exp.Kind)
	}
	return nil
}

func runSurface(scalar func(string, ...any) (any, error), prefix string) error {
	raw, err := os.ReadFile(catalogPath())
	if err != nil {
		return err
	}
	var cat catalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		return err
	}
	vars := map[string]any{}
	for _, st := range cat.Steps {
		args := make([]any, len(st.Args))
		for i, a := range st.Args {
			args[i], err = resolve(a, prefix, vars)
			if err != nil {
				return fmt.Errorf("%s: %w", st.ID, err)
			}
		}
		result, err := scalar(st.SQL, args...)
		if err != nil {
			return fmt.Errorf("%s failed: %w", st.ID, err)
		}
		if st.Store != "" {
			vars[st.Store] = result
		}
		if st.Expect != nil {
			if err := check(*st.Expect, result, prefix, vars); err != nil {
				return fmt.Errorf("%s: %w", st.ID, err)
			}
		}
	}
	return nil
}

func stdScalar(db *sql.DB) func(string, ...any) (any, error) {
	return func(q string, args ...any) (any, error) {
		var v any
		if err := db.QueryRow(q, args...).Scan(&v); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func gormScalar(db *gorm.DB) func(string, ...any) (any, error) {
	return func(q string, args ...any) (any, error) {
		var v any
		if err := db.Raw(q, args...).Row().Scan(&v); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func stdAtomicity(db *sql.DB, queue string, commitID, rollbackID int) error {
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)"); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO orders (id, user_id) VALUES (?, ?)", commitID, 1); err != nil {
		tx.Rollback()
		return err
	}
	var jobID int64
	if err := tx.QueryRow(
		"SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
		queue, fmt.Sprintf(`{"order_id":%d}`, commitID), 0, 3,
	).Scan(&jobID); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM orders WHERE id = ?", commitID).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("expected committed order %d", commitID)
	}
	var job string
	if err := db.QueryRow("SELECT honker_get_job(?)", jobID).Scan(&job); err != nil {
		return err
	}
	if !strings.Contains(job, "order_id") {
		return fmt.Errorf("missing committed job")
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO orders (id, user_id) VALUES (?, ?)", rollbackID, 1); err != nil {
		tx.Rollback()
		return err
	}
	var rolled int64
	if err := tx.QueryRow(
		"SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
		queue, fmt.Sprintf(`{"order_id":%d}`, rollbackID), 0, 3,
	).Scan(&rolled); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Rollback(); err != nil {
		return err
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM orders WHERE id = ?", rollbackID).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("rollback left order %d", rollbackID)
	}
	if err := db.QueryRow("SELECT honker_get_job(?)", rolled).Scan(&job); err != nil {
		return err
	}
	if job != "" {
		return fmt.Errorf("rollback left job %q", job)
	}
	return nil
}

func gormAtomicity(db *gorm.DB, queue string, commitID, rollbackID int) error {
	if err := db.Exec("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER)").Error; err != nil {
		return err
	}
	var jobID int64
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO orders (id, user_id) VALUES (?, ?)", commitID, 1).Error; err != nil {
			return err
		}
		return tx.Raw(
			"SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
			queue, fmt.Sprintf(`{"order_id":%d}`, commitID), 0, 3,
		).Scan(&jobID).Error
	})
	if err != nil {
		return err
	}
	var n int64
	if err := db.Raw("SELECT COUNT(*) FROM orders WHERE id = ?", commitID).Scan(&n).Error; err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("expected committed order %d", commitID)
	}
	var job string
	if err := db.Raw("SELECT honker_get_job(?)", jobID).Scan(&job).Error; err != nil {
		return err
	}
	if !strings.Contains(job, "order_id") {
		return fmt.Errorf("missing committed job")
	}

	var rolled int64
	_ = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO orders (id, user_id) VALUES (?, ?)", rollbackID, 1).Error; err != nil {
			return err
		}
		if err := tx.Raw(
			"SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
			queue, fmt.Sprintf(`{"order_id":%d}`, rollbackID), 0, 3,
		).Scan(&rolled).Error; err != nil {
			return err
		}
		return fmt.Errorf("rollback")
	})
	if err := db.Raw("SELECT COUNT(*) FROM orders WHERE id = ?", rollbackID).Scan(&n).Error; err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("rollback left order %d", rollbackID)
	}
	if err := db.Raw("SELECT honker_get_job(?)", rolled).Scan(&job).Error; err != nil {
		return err
	}
	if job != "" {
		return fmt.Errorf("rollback left job %q", job)
	}
	return nil
}

func main() {
	dbPath := os.Getenv("HONKER_TEST_DB")
	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "HONKER_TEST_DB is required")
		os.Exit(1)
	}

	stdDB, err := sql.Open("sqlite3_honker", "file:"+dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "database/sql open:", err)
		os.Exit(1)
	}
	if _, err := stdDB.Exec("SELECT honker_bootstrap()"); err != nil {
		fmt.Fprintln(os.Stderr, "bootstrap:", err)
		os.Exit(1)
	}
	if err := runSurface(stdScalar(stdDB), "gs"); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL database/sql:", err)
		os.Exit(1)
	}
	if err := stdAtomicity(stdDB, "gs_atomic", 42, 43); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL database/sql atomicity:", err)
		os.Exit(1)
	}
	fmt.Println("PASS go-database-sql")
	stdDB.Close()

	gormDB, err := gorm.Open(
		sqlite.Dialector{DriverName: "sqlite3_honker", DSN: "file:" + dbPath},
		&gorm.Config{},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gorm open:", err)
		os.Exit(1)
	}
	if err := runSurface(gormScalar(gormDB), "gg"); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL gorm:", err)
		os.Exit(1)
	}
	if err := gormAtomicity(gormDB, "gg_atomic", 52, 53); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL gorm atomicity:", err)
		os.Exit(1)
	}
	fmt.Println("PASS go-gorm")
}
