package unionstore

import (
	art "github.com/tikv/client-go/v2/internal/unionstore/art"
)

type ArenaArt struct {
	*art.Art
}

func NewArenaArt() *ArenaArt {
	return &ArenaArt{
		Art: art.New(),
	}
}
