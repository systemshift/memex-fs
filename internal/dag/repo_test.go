package dag

import (
	"strings"
	"testing"
)

func openTestRepo(t *testing.T) *Repository {
	t.Helper()
	dir := t.TempDir()
	repo, err := OpenRepository(dir)
	if err != nil {
		t.Fatalf("OpenRepository: %v", err)
	}
	return repo
}

func TestCreateNode_GetNode(t *testing.T) {
	repo := openTestRepo(t)

	meta := map[string]interface{}{"format": "text"}
	created, err := repo.CreateNode("test-1", "Note", []byte("hello"), meta)
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if created.ID != "test-1" {
		t.Errorf("ID = %q, want %q", created.ID, "test-1")
	}
	if created.Type != "Note" {
		t.Errorf("Type = %q, want %q", created.Type, "Note")
	}

	got, err := repo.GetNode("test-1")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if string(got.Content) != "hello" {
		t.Errorf("Content = %q, want %q", got.Content, "hello")
	}
	if got.Meta["format"] != "text" {
		t.Errorf("Meta[format] = %v, want %q", got.Meta["format"], "text")
	}
}

func TestUpdateContent(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("uc-1", "Note", []byte("original"), nil)

	updated, err := repo.UpdateContent("uc-1", []byte("modified"))
	if err != nil {
		t.Fatalf("UpdateContent: %v", err)
	}
	if string(updated.Content) != "modified" {
		t.Errorf("Content = %q, want %q", updated.Content, "modified")
	}

	got, err := repo.GetNode("uc-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Content) != "modified" {
		t.Errorf("re-read Content = %q, want %q", got.Content, "modified")
	}
}

func TestUpdateNode_MetaMerge(t *testing.T) {
	repo := openTestRepo(t)

	meta := map[string]interface{}{"a": "1", "b": "2"}
	repo.CreateNode("um-1", "Note", []byte("x"), meta)

	// Patch: update a, delete b, add c
	updated, err := repo.UpdateNode("um-1", map[string]interface{}{
		"a": "changed",
		"b": nil,
		"c": "new",
	})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if updated.Meta["a"] != "changed" {
		t.Errorf("Meta[a] = %v, want %q", updated.Meta["a"], "changed")
	}
	if _, ok := updated.Meta["b"]; ok {
		t.Errorf("Meta[b] should be deleted, got %v", updated.Meta["b"])
	}
	if updated.Meta["c"] != "new" {
		t.Errorf("Meta[c] = %v, want %q", updated.Meta["c"], "new")
	}
}

func TestDeleteNode_Soft(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("sd-1", "Note", []byte("bye"), nil)

	if err := repo.DeleteNode("sd-1", false); err != nil {
		t.Fatalf("DeleteNode(soft): %v", err)
	}

	_, err := repo.GetNode("sd-1")
	if err == nil {
		t.Error("expected error after soft delete")
	}

	// Ref still exists (tombstone)
	if !repo.Refs.Has("sd-1") {
		t.Error("ref should still exist after soft delete")
	}
}

func TestDeleteNode_Hard(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("hd-1", "Note", []byte("gone"), nil)

	if err := repo.DeleteNode("hd-1", true); err != nil {
		t.Fatalf("DeleteNode(hard): %v", err)
	}

	if repo.Refs.Has("hd-1") {
		t.Error("ref should be gone after hard delete")
	}
}

func TestCreateLink_GetLinks(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("ln-a", "Note", []byte("a"), nil)
	repo.CreateNode("ln-b", "Note", []byte("b"), nil)

	if err := repo.CreateLink("ln-a", "ln-b", "references"); err != nil {
		t.Fatalf("CreateLink: %v", err)
	}

	linksA := repo.GetLinks("ln-a")
	if len(linksA) != 1 {
		t.Fatalf("links from ln-a: got %d, want 1", len(linksA))
	}
	if linksA[0].Target != "ln-b" {
		t.Errorf("link target = %q, want %q", linksA[0].Target, "ln-b")
	}
	if linksA[0].Type != "references" {
		t.Errorf("link type = %q, want %q", linksA[0].Type, "references")
	}

	linksB := repo.GetLinks("ln-b")
	if len(linksB) != 1 {
		t.Fatalf("links from ln-b: got %d, want 1", len(linksB))
	}
	if linksB[0].Source != "ln-a" {
		t.Errorf("reverse link source = %q, want %q", linksB[0].Source, "ln-a")
	}
}

func TestSearchNodes(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("sr-1", "Note", []byte("the quick brown fox"), nil)
	repo.CreateNode("sr-2", "Note", []byte("lazy dog sleeps"), nil)

	results, err := repo.SearchNodes("quick fox", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least 1 search result")
	}
	if results[0].ID != "sr-1" {
		t.Errorf("top result ID = %q, want %q", results[0].ID, "sr-1")
	}
}

func TestFilterNodes(t *testing.T) {
	repo := openTestRepo(t)

	repo.CreateNode("ft-1", "Note", []byte("a"), nil)
	repo.CreateNode("ft-2", "Task", []byte("b"), nil)
	repo.CreateNode("ft-3", "Note", []byte("c"), nil)

	notes, err := repo.FilterNodes("Note", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 2 {
		t.Fatalf("Note filter: got %d, want 2", len(notes))
	}

	tasks, err := repo.FilterNodes("Task", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Task filter: got %d, want 1", len(tasks))
	}
}

func TestListNodes(t *testing.T) {
	repo := openTestRepo(t)

	for i := 0; i < 5; i++ {
		repo.CreateNode("ls-"+string(rune('a'+i)), "Note", []byte("x"), nil)
	}

	all, err := repo.ListNodes(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Errorf("ListNodes(0): got %d, want 5", len(all))
	}

	limited, err := repo.ListNodes(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 3 {
		t.Errorf("ListNodes(3): got %d, want 3", len(limited))
	}
}

func TestIngest_Dedup(t *testing.T) {
	repo := openTestRepo(t)

	id1, created1, err := repo.Ingest("dedup content", "text")
	if err != nil {
		t.Fatal(err)
	}
	if !created1 {
		t.Error("first ingest should be new")
	}
	if !strings.HasPrefix(id1, "sha256:") {
		t.Errorf("id should start with sha256:, got %q", id1)
	}

	id2, created2, err := repo.Ingest("dedup content", "text")
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Error("second ingest of same content should not be new")
	}
	if id1 != id2 {
		t.Errorf("dedup IDs differ: %q vs %q", id1, id2)
	}
}

func TestGetNode_NotFound(t *testing.T) {
	repo := openTestRepo(t)
	_, err := repo.GetNode("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestUpdateContent_DeletedNode(t *testing.T) {
	repo := openTestRepo(t)
	repo.CreateNode("del-1", "Note", []byte("x"), nil)
	repo.DeleteNode("del-1", false)

	_, err := repo.UpdateContent("del-1", []byte("y"))
	if err == nil {
		t.Error("expected error updating deleted node")
	}
}
