package dagit

import (
	"crypto/sha256"
	"fmt"
)

var adjectives = [64]string{
	"amber", "azure", "bold", "bright", "calm", "clear", "cool", "coral",
	"crimson", "dark", "deep", "dry", "dusk", "faint", "fast", "firm",
	"gold", "green", "grey", "haze", "iron", "keen", "kind", "late",
	"light", "live", "long", "loud", "low", "mild", "mint", "mist",
	"moss", "near", "new", "next", "north", "odd", "old", "open",
	"pale", "pine", "plain", "proud", "pure", "quick", "quiet", "rare",
	"raw", "red", "rich", "sage", "salt", "sand", "sharp", "shy",
	"silk", "slim", "slow", "soft", "south", "steel", "still", "stone",
}

var nouns = [64]string{
	"ash", "bay", "birch", "bloom", "brook", "cave", "cedar", "cliff",
	"cloud", "coal", "cove", "crane", "creek", "crow", "dawn", "deer",
	"dove", "dune", "dusk", "eagle", "elm", "ember", "fern", "finch",
	"fire", "flint", "fox", "frost", "gale", "glen", "grove", "hawk",
	"haze", "heath", "heron", "hill", "ivy", "jade", "jay", "lake",
	"lark", "leaf", "marsh", "mesa", "moon", "oak", "owl", "peak",
	"pine", "pond", "rain", "reed", "ridge", "rock", "rose", "sage",
	"shade", "shore", "sky", "snow", "star", "storm", "stone", "vale",
}

// PetnameFromDID generates a deterministic adjective-noun name from a DID.
func PetnameFromDID(did string) string {
	h := sha256.Sum256([]byte(did))
	adj := adjectives[h[0]%64]
	noun := nouns[h[1]%64]
	return fmt.Sprintf("%s-%s", adj, noun)
}
