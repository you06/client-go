package art

type tree struct {
	root *artNode
}

func New() *tree {
	return &tree{}
}

func (t *tree) Set(key, value []byte) error {
	return nil
}

func (t *tree) Delete(key []byte) error {
	return nil
}

func (t *tree) set(key, value []byte) error {
	return nil
}

func (t *tree) Get(key []byte) ([]byte, error) {
	return nil, nil
}
