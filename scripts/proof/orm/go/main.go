// database/sql and GORM, as guides/orm/go.mdx shows them.
//
// Both go through the mattn/go-sqlite3 driver variant registered with
// the Extensions field, which is the documented wiring. GORM is not a
// separate loading path — it wraps database/sql — but it is documented
// separately, so it is proven separately rather than assumed.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	honker "github.com/russellromney/honker-go"
	sqlite3 "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type job struct {
	ID      int64  `json:"id"`
	Payload string `json:"payload"`
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

// roundTrip runs enqueue -> claim -> ack with every value bound, never
// inlined, so the driver's own parameter binding is what is exercised.
func roundTrip(exec func(string, ...any) (*sql.Row, error), label string) error {
	row, err := exec("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)", label, `{"to":"alice"}`, 0, 3)
	if err != nil {
		return err
	}
	var id int64
	if err := row.Scan(&id); err != nil {
		return err
	}
	if id <= 0 {
		return fmt.Errorf("expected a job id, got %d", id)
	}

	row, err = exec("SELECT honker_claim_batch(?, ?, ?, ?)", label, "w1", 8, 300)
	if err != nil {
		return err
	}
	var claimedJSON string
	if err := row.Scan(&claimedJSON); err != nil {
		return err
	}
	var claimed []job
	if err := json.Unmarshal([]byte(claimedJSON), &claimed); err != nil {
		return err
	}
	if len(claimed) != 1 || claimed[0].ID != id {
		return fmt.Errorf("expected to claim job %d, got %s", id, claimedJSON)
	}

	row, err = exec("SELECT honker_ack(?, ?)", id, "w1")
	if err != nil {
		return err
	}
	var ok int
	if err := row.Scan(&ok); err != nil {
		return err
	}
	if ok != 1 {
		return fmt.Errorf("ack must match the claim, got %d", ok)
	}
	return nil
}

func main() {
	dbPath := os.Getenv("HONKER_TEST_DB")
	if dbPath == "" {
		fmt.Fprintln(os.Stderr, "HONKER_TEST_DB is required")
		os.Exit(1)
	}

	// database/sql
	stdDB, err := sql.Open("sqlite3_honker", "file:"+dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "database/sql open:", err)
		os.Exit(1)
	}
	if _, err := stdDB.Exec("SELECT honker_bootstrap()"); err != nil {
		fmt.Fprintln(os.Stderr, "bootstrap:", err)
		os.Exit(1)
	}
	stdExec := func(q string, args ...any) (*sql.Row, error) { return stdDB.QueryRow(q, args...), nil }
	if err := roundTrip(stdExec, "emails_std"); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL database/sql:", err)
		os.Exit(1)
	}
	fmt.Println("PASS go-database-sql")
	stdDB.Close()

	// GORM over the same registered driver
	gormDB, err := gorm.Open(
		sqlite.Dialector{DriverName: "sqlite3_honker", DSN: "file:" + dbPath},
		&gorm.Config{},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gorm open:", err)
		os.Exit(1)
	}
	gormExec := func(q string, args ...any) (*sql.Row, error) { return gormDB.Raw(q, args...).Row(), nil }
	if err := roundTrip(gormExec, "emails_gorm"); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL gorm:", err)
		os.Exit(1)
	}
	fmt.Println("PASS go-gorm")
}
