package uniqueset

type UniqueSet struct {
	values map[string]struct{}
}

func New() *UniqueSet {
	return &UniqueSet{
		values: make(map[string]struct{}),
	}
}

func (us *UniqueSet) Add(value string) bool {
	if _, exists := us.values[value]; exists {
		return false
	}
	us.values[value] = struct{}{}
	return true
}

func (us *UniqueSet) Exists(value string) bool {
	_, exists := us.values[value]
	return exists
}
