package traffictype

import "testing"

// TestAllMatchesConstants — страховка от рассинхрона: если кто-то добавит
// четвёртую константу и забудет внести её в All (или наоборот), тест это
// заметит явно, а не тихо разойдётся с CHECK-констрейнтом миграции 016.
func TestAllMatchesConstants(t *testing.T) {
	want := map[string]bool{Human: true, Bot: true, Internal: true}
	if len(All) != len(want) {
		t.Fatalf("len(All) = %d; want %d", len(All), len(want))
	}
	for k := range want {
		if !All[k] {
			t.Errorf("All[%q] = false; want true", k)
		}
	}
}
