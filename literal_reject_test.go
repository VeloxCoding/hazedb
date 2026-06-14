package hazedb

import (
	"errors"
	"testing"
)

// Inline value literals are rejected at prepare so every value flows through a ?
// placeholder — this bounds the plan cache (only finite parameterized shapes are
// cached) and removes the SQL-injection path. Placeholders, IS NULL/IS NOT NULL,
// and the structural LIMIT/OFFSET ints stay valid.
func TestRejectInlineValueLiterals(t *testing.T) {
	db := openMem(t) // users(id uuid, name text, age int, active bool)

	rejected := []string{
		"SELECT id FROM users WHERE name = 'alice'",              // string value
		"SELECT id FROM users WHERE age = 30",                    // int value
		"SELECT id FROM users WHERE active = true",               // bool value
		"SELECT id FROM users WHERE age + 1 = ?",                 // literal inside arithmetic
		"SELECT id FROM users WHERE age >= 0 LIMIT 5",            // value literal (LIMIT itself is fine)
		"UPDATE users SET age = 31 WHERE id = ?",                 // SET value
		"DELETE FROM users WHERE name = 'alice'",                 // DELETE WHERE value
		"INSERT INTO users (id, name, age) VALUES (?, 'bob', ?)", // VALUES literal
	}
	for _, sql := range rejected {
		if _, err := db.prepare(sql, db.cat.Load()); !errors.Is(err, ErrParse) {
			t.Errorf("expected rejection for %q, got %v", sql, err)
		}
	}

	accepted := []string{
		"SELECT id FROM users WHERE name = ?",
		"SELECT id FROM users WHERE age IS NULL",
		"SELECT id FROM users WHERE age IS NOT NULL",
		"SELECT name, age FROM users WHERE age >= ? LIMIT 5 OFFSET 2", // LIMIT/OFFSET are structural
		"SELECT name FROM users ORDER BY age DESC LIMIT 2",
		"UPDATE users SET age = ? WHERE id = ?",
		"DELETE FROM users WHERE id = ?",
		"INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
		"SELECT id, name FROM users", // no WHERE, no values
	}
	for _, sql := range accepted {
		if _, err := db.prepare(sql, db.cat.Load()); err != nil {
			t.Errorf("expected %q to be accepted, got %v", sql, err)
		}
	}
}
