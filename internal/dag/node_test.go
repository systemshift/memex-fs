package dag

import (
	"encoding/json"
	"testing"
)

func TestCanonicalJSON_SortedKeys(t *testing.T) {
	input := map[string]interface{}{"b": 1, "a": 2}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":2,"b":1}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSON_CompactEncoding(t *testing.T) {
	input := map[string]interface{}{"key": "value", "num": 42}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	// Must have no spaces after : or ,
	s := string(got)
	for i, c := range s {
		if c == ':' && i > 0 && s[i-1] == ' ' {
			t.Error("space before colon")
		}
		if c == ',' && i+1 < len(s) && s[i+1] == ' ' {
			t.Error("space after comma")
		}
	}
	want := `{"key":"value","num":42}`
	if s != want {
		t.Errorf("got %s, want %s", s, want)
	}
}

func TestCanonicalJSON_NestedObjects(t *testing.T) {
	input := map[string]interface{}{
		"z": map[string]interface{}{
			"b": 1,
			"a": 2,
		},
		"a": "first",
	}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":"first","z":{"a":2,"b":1}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSON_ArraysPreserved(t *testing.T) {
	input := map[string]interface{}{
		"arr": []interface{}{3, 1, 2},
	}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"arr":[3,1,2]}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSON_EmptyArrays(t *testing.T) {
	// Ensure []string{} → [] not null.
	// Go's json.Marshal converts []string{} to [].
	input := map[string]interface{}{
		"refs": []string{},
		"tags": []string{},
	}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"refs":[],"tags":[]}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSON_NilSliceBecomesNull(t *testing.T) {
	// This is a gotcha: nil slices marshal as null.
	// Code must use []string{} not nil to get [].
	type Foo struct {
		Items []string `json:"items"`
	}
	got, err := CanonicalJSON(Foo{Items: nil})
	if err != nil {
		t.Fatal(err)
	}
	s := string(got)
	// nil slice → "null" in JSON
	if s != `{"items":null}` {
		t.Errorf("nil slice: got %s", s)
	}

	got2, err := CanonicalJSON(Foo{Items: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != `{"items":[]}` {
		t.Errorf("empty slice: got %s", got2)
	}
}

func TestCanonicalJSON_PythonEquivalence(t *testing.T) {
	// Exact output of:
	//   json.dumps({"v":2,"type":"post","content":"hello from test",
	//     "author":"did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd",
	//     "refs":[],"tags":[],"timestamp":"2024-01-01T00:00:00Z"},
	//     sort_keys=True, separators=(",",":"))
	want := `{"author":"did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd","content":"hello from test","refs":[],"tags":[],"timestamp":"2024-01-01T00:00:00Z","type":"post","v":2}`

	input := map[string]interface{}{
		"v":         2,
		"type":      "post",
		"content":   "hello from test",
		"author":    "did:key:z6MkehRgf7yJbgaGfYsdoAsKdBPE3dj2CYhowQdcjqSJgvVd",
		"refs":      []string{},
		"tags":      []string{},
		"timestamp": "2024-01-01T00:00:00Z",
	}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("Python equivalence failed\n  got:  %s\n  want: %s", got, want)
	}
}

func TestCanonicalJSON_Deterministic(t *testing.T) {
	// Same input must always produce same output.
	input := map[string]interface{}{
		"c": 3, "a": 1, "b": 2,
		"nested": map[string]interface{}{"z": true, "a": false},
	}
	first, _ := CanonicalJSON(input)
	for i := 0; i < 50; i++ {
		got, _ := CanonicalJSON(input)
		if string(got) != string(first) {
			t.Fatalf("non-deterministic on iteration %d:\n  first: %s\n  got:   %s", i, first, got)
		}
	}
}

func TestCanonicalJSON_SpecialCharacters(t *testing.T) {
	input := map[string]interface{}{
		"msg": "hello \"world\"\nnewline",
	}
	got, err := CanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	// Verify it's valid JSON
	var check map[string]interface{}
	if err := json.Unmarshal(got, &check); err != nil {
		t.Fatalf("output is not valid JSON: %s", got)
	}
	if check["msg"] != "hello \"world\"\nnewline" {
		t.Errorf("round-trip value mismatch: %v", check["msg"])
	}
}
