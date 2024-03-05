package collection

// Map manipulates a slice and transforms it to a slice of another type.
func Map[T any, R any](collection []T, callback func(item T, index int) R) []R {
	result := make([]R, len(collection))

	for i, item := range collection {
		result[i] = callback(item, i)
	}

	return result
}

func ForEach[T any](collection []T, callback func(item T, idx int) error) error {
	for idx, c := range collection {
		if err := callback(c, idx); err != nil {
			return err
		}
	}
	return nil
}
