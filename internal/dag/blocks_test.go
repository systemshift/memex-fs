package dag

import "testing"

func TestBlocks_Empty(t *testing.T) {
	if got := Blocks(nil); len(got) != 0 {
		t.Errorf("nil content = %v, want empty", got)
	}
	if got := Blocks([]byte("")); len(got) != 0 {
		t.Errorf("empty content = %v, want empty", got)
	}
	if got := Blocks([]byte("\n\n  \n")); len(got) != 0 {
		t.Errorf("whitespace-only content = %v, want empty", got)
	}
}

func TestBlocks_SingleParagraph(t *testing.T) {
	got := Blocks([]byte("one line only"))
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Index != 1 || got[0].Text != "one line only" {
		t.Errorf("block = %+v, want {1, \"one line only\"}", got[0])
	}
}

func TestBlocks_MultipleParagraphs(t *testing.T) {
	content := "first para\nstill first\n\nsecond para\n\n\nthird para"
	got := Blocks([]byte(content))
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3; got = %+v", len(got), got)
	}
	if got[0].Text != "first para\nstill first" {
		t.Errorf("block 1 = %q", got[0].Text)
	}
	if got[1].Text != "second para" {
		t.Errorf("block 2 = %q", got[1].Text)
	}
	if got[2].Text != "third para" {
		t.Errorf("block 3 = %q", got[2].Text)
	}
	for i, b := range got {
		if b.Index != i+1 {
			t.Errorf("block[%d].Index = %d, want %d", i, b.Index, i+1)
		}
	}
}

func TestBlocks_IgnoresTrailingBlankLines(t *testing.T) {
	got := Blocks([]byte("hello\n\n\n\n"))
	if len(got) != 1 {
		t.Errorf("len = %d, want 1", len(got))
	}
}

func TestBlocks_HandlesCRLF(t *testing.T) {
	got := Blocks([]byte("a\r\n\r\nb"))
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Text != "a" || got[1].Text != "b" {
		t.Errorf("blocks = %+v, want [a, b]", got)
	}
}
