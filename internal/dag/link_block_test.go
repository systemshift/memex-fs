package dag

import "testing"

func TestLinkTargetParent(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"paper:abc", "paper:abc"},
		{"paper:abc#b3", "paper:abc"},
		{"paper:abc#b42", "paper:abc"},
		{"person:alice#b1", "person:alice"},
		{"", ""},
		// Leading # is not a block suffix — it's the whole "target"
		{"#weird", "#weird"},
	}
	for _, c := range cases {
		if got := LinkTargetParent(c.in); got != c.want {
			t.Errorf("LinkTargetParent(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestLinksTo_SurfacesBlockScopedTargets(t *testing.T) {
	repo := openTestRepo(t)

	if _, err := repo.CreateNode("paper:1", "Paper", []byte("para one\n\npara two\n\npara three"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNode("person:alice", "Person", nil, nil); err != nil {
		t.Fatal(err)
	}

	// Link alice -> paper:1#b2 (block-scoped).
	if err := repo.CreateLink("person:alice", "paper:1#b2", "cites"); err != nil {
		t.Fatal(err)
	}

	// paper:1's backlinks should surface alice even though the target
	// was block-scoped.
	in := repo.Links.LinksTo("paper:1")
	if len(in) != 1 {
		t.Fatalf("LinksTo(paper:1) = %d, want 1; got %+v", len(in), in)
	}
	if in[0].Source != "person:alice" || in[0].Target != "paper:1#b2" {
		t.Errorf("backlink = %+v", in[0])
	}
}

func TestLinksTo_DirectAndBlockMerge(t *testing.T) {
	repo := openTestRepo(t)

	for _, id := range []string{"paper:1", "person:alice", "person:bob"} {
		if _, err := repo.CreateNode(id, "N", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	// alice links to the whole paper; bob links to block 3.
	if err := repo.CreateLink("person:alice", "paper:1", "cites"); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateLink("person:bob", "paper:1#b3", "cites"); err != nil {
		t.Fatal(err)
	}

	in := repo.Links.LinksTo("paper:1")
	if len(in) != 2 {
		t.Errorf("expected 2 backlinks (direct + block-scoped), got %d: %+v", len(in), in)
	}
}
