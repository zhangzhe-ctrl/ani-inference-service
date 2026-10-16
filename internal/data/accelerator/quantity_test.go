package accelerator

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/liangzai006/ani-inference-service/internal/biz/gpu"
	acceleratorv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/v1"
)

func TestQuantityProtoAdapterPreservesCompleteBuilderPlan(t *testing.T) {
	raw, err := os.ReadFile("../../../testdata/accelerator-quantity-plans.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]json.RawMessage
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Whole1", "Whole2", "Shared1", "WholeProfileMax"} {
		t.Run(name, func(t *testing.T) {
			var input acceleratorv1.ResolvedGpuPlan
			var want gpu.Plan
			if err := json.Unmarshal(vectors[name], &input); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(vectors[name], &want); err != nil {
				t.Fatal(err)
			}
			got, err := fromProtoPlan(&input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, &want) {
				t.Fatalf("protobuf adapter dropped plan fields: got=%+v want=%+v", got, want)
			}
			if err := gpu.ValidateManagedPlan(got, got.Request); err != nil {
				t.Fatal(err)
			}
		})
	}
}
